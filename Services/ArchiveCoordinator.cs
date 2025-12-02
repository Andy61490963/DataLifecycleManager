using System.Dynamic;
using System.Globalization;
using System.Transactions;
using Dapper;
using DataLifecycleManager.Configuration;
using DataLifecycleManager.Infrastructure;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DataLifecycleManager.Services;

/// <summary>
/// 整合線上庫搬移與 CSV 歷史匯出的流程控制器。
/// </summary>
public class ArchiveCoordinator
{
    private readonly ArchiveSettings _settings;
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly RetryPolicyExecutor _retryExecutor;
    private readonly ILogger<ArchiveCoordinator> _logger;

    /// <summary>
    /// 建構子注入必要元件。
    /// </summary>
    /// <param name="options">歸檔設定。</param>
    /// <param name="connectionFactory">連線工廠。</param>
    /// <param name="retryExecutor">重試封裝。</param>
    /// <param name="logger">紀錄器。</param>
    public ArchiveCoordinator(IOptions<ArchiveSettings> options, SqlConnectionFactory connectionFactory, RetryPolicyExecutor retryExecutor, ILogger<ArchiveCoordinator> logger)
    {
        _settings = options.Value;
        _connectionFactory = connectionFactory;
        _retryExecutor = retryExecutor;
        _logger = logger;
    }

    /// <summary>
    /// 執行一次完整排程，涵蓋 DB1→DB2 歸檔與 DB2→CSV 匯出。
    /// </summary>
    /// <param name="cancellationToken">取消權杖。</param>
    public async Task RunOnceAsync(CancellationToken cancellationToken)
    {
        if (!_settings.Enabled)
        {
            _logger.LogInformation("歸檔排程已停用，略過本次執行。");
            return;
        }

        foreach (var table in _settings.Tables)
        {
            var effective = BuildEffectiveTableSettings(table);
            await _retryExecutor.ExecuteAsync($"{table.Name}-Archive", () => ArchiveOnlineAsync(effective, cancellationToken), cancellationToken);

            if (_settings.Csv.Enabled && effective.CsvEnabled)
            {
                await _retryExecutor.ExecuteAsync($"{table.Name}-Csv", () => ExportHistoryAsync(effective, cancellationToken), cancellationToken);
            }
        }
    }

    /// <summary>
    /// 針對單一資料表搬移超過保留期的線上資料到歷史庫。
    /// </summary>
    /// <param name="table">合併後的表設定。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    private async Task ArchiveOnlineAsync(EffectiveTableSettings table, CancellationToken cancellationToken)
    {
        var cutoff = DateTime.UtcNow.AddMonths(-table.OnlineRetentionMonths);
        _logger.LogInformation("開始搬移 {Table}，截止日 {Cutoff:yyyy-MM-dd}，批次 {BatchSize}", table.Name, cutoff, table.BatchSize);

        while (!cancellationToken.IsCancellationRequested)
        {
            var rows = await FetchBatchAsync(table.SourceConnectionName, table.Name, table.DateColumn, cutoff, table.BatchSize, cancellationToken);
            if (!rows.Any())
            {
                break;
            }

            await MoveBatchAsync(table, rows, cancellationToken);
        }
    }

    /// <summary>
    /// 針對歷史庫資料超過保留期時匯出 CSV 並刪除。
    /// </summary>
    /// <param name="table">合併後的表設定。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    private async Task ExportHistoryAsync(EffectiveTableSettings table, CancellationToken cancellationToken)
    {
        var cutoff = DateTime.UtcNow.AddMonths(-table.HistoryRetentionMonths);
        _logger.LogInformation("開始 CSV 匯出 {Table}，截止日 {Cutoff:yyyy-MM-dd}，批次 {BatchSize}", table.Name, cutoff, table.BatchSize);

        while (!cancellationToken.IsCancellationRequested)
        {
            var rows = await FetchBatchAsync(table.TargetConnectionName, table.Name, table.DateColumn, cutoff, table.BatchSize, cancellationToken);
            if (!rows.Any())
            {
                break;
            }

            var (fromDate, toDate) = CalculateRange(rows, table.DateColumn);
            await WriteCsvFilesAsync(rows, table, fromDate, toDate, cancellationToken);
            await DeleteBatchAsync(table.TargetConnectionName, table.Name, table.PrimaryKeyColumn, rows, cancellationToken);
        }
    }

    /// <summary>
    /// 將全域預設值與表層設定合併，取得完整運行參數。
    /// </summary>
    /// <param name="table">原始表設定。</param>
    private EffectiveTableSettings BuildEffectiveTableSettings(TableSettings table)
    {
        return new EffectiveTableSettings
        {
            Name = table.Name,
            SourceConnectionName = table.SourceConnectionName,
            TargetConnectionName = table.TargetConnectionName,
            DateColumn = table.DateColumn,
            PrimaryKeyColumn = table.PrimaryKeyColumn,
            OnlineRetentionMonths = table.OnlineRetentionMonths ?? _settings.DefaultOnlineRetentionMonths,
            HistoryRetentionMonths = table.HistoryRetentionMonths ?? _settings.DefaultHistoryRetentionMonths,
            BatchSize = table.BatchSize ?? _settings.DefaultBatchSize,
            CsvEnabled = table.CsvEnabled
        };
    }

    /// <summary>
    /// 依據條件抓取指定資料表的批次資料。
    /// </summary>
    /// <param name="connectionName">連線名稱。</param>
    /// <param name="tableName">資料表名稱。</param>
    /// <param name="dateColumn">日期欄位。</param>
    /// <param name="cutoff">截止時間。</param>
    /// <param name="batchSize">批次大小。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    private async Task<IReadOnlyList<dynamic>> FetchBatchAsync(string connectionName, string tableName, string dateColumn, DateTime cutoff, int batchSize, CancellationToken cancellationToken)
    {
        var sql = $"SELECT TOP (@BatchSize) * FROM {Bracket(tableName)} WITH (READPAST) WHERE {Bracket(dateColumn)} < @Cutoff ORDER BY {Bracket(dateColumn)} ASC";
        await using var connection = _connectionFactory.CreateConnection(connectionName);
        await connection.OpenAsync(cancellationToken);

        var rows = await connection.QueryAsync(sql, new { BatchSize = batchSize, Cutoff = cutoff });
        return rows.ToList();
    }

    /// <summary>
    /// 使用 TransactionScope 將單批資料搬移至目標庫並刪除來源。
    /// </summary>
    /// <param name="table">合併後的設定。</param>
    /// <param name="rows">待搬移資料。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    private async Task MoveBatchAsync(EffectiveTableSettings table, IReadOnlyList<dynamic> rows, CancellationToken cancellationToken)
    {
        var columns = DynamicSqlHelper.GetColumnNames(rows.First());
        var insertSql = DynamicSqlHelper.BuildInsertSql(table.Name, columns, table.PrimaryKeyColumn);
        var deleteSql = DynamicSqlHelper.BuildDeleteSql(table.Name, table.PrimaryKeyColumn);
        var primaryKeys = rows.Select(r => ((IDictionary<string, object>)r)[table.PrimaryKeyColumn]).ToList();

        using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

        await using var target = _connectionFactory.CreateConnection(table.TargetConnectionName);
        await target.OpenAsync(cancellationToken);
        await target.ExecuteAsync(insertSql, rows);

        await using var source = _connectionFactory.CreateConnection(table.SourceConnectionName);
        await source.OpenAsync(cancellationToken);
        await source.ExecuteAsync(deleteSql, new { Ids = primaryKeys });

        scope.Complete();
        _logger.LogInformation("{Table} 搬移 {Count} 筆完成。", table.Name, rows.Count);
    }

    /// <summary>
    /// 以主鍵批次刪除指定資料。
    /// </summary>
    /// <param name="connectionName">連線名稱。</param>
    /// <param name="tableName">資料表。</param>
    /// <param name="primaryKeyColumn">主鍵欄位。</param>
    /// <param name="rows">目標資料列。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    private async Task DeleteBatchAsync(string connectionName, string tableName, string primaryKeyColumn, IReadOnlyList<dynamic> rows, CancellationToken cancellationToken)
    {
        var deleteSql = DynamicSqlHelper.BuildDeleteSql(tableName, primaryKeyColumn);
        var primaryKeys = rows.Select(r => ((IDictionary<string, object>)r)[primaryKeyColumn]).ToList();

        await using var connection = _connectionFactory.CreateConnection(connectionName);
        await connection.OpenAsync(cancellationToken);
        await connection.ExecuteAsync(deleteSql, new { Ids = primaryKeys });
        _logger.LogInformation("{Table} 刪除 {Count} 筆歷史資料。", tableName, rows.Count);
    }

    /// <summary>
    /// 將批次資料依分段規則輸出為 CSV 檔案。
    /// </summary>
    /// <param name="rows">資料列。</param>
    /// <param name="table">表設定。</param>
    /// <param name="fromDate">批次最早日期。</param>
    /// <param name="toDate">批次最晚日期。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    private async Task WriteCsvFilesAsync(IReadOnlyList<dynamic> rows, EffectiveTableSettings table, DateTime fromDate, DateTime toDate, CancellationToken cancellationToken)
    {
        var columns = DynamicSqlHelper.GetColumnNames(rows.First());
        var delimiter = _settings.Csv.Delimiter;
        var chunks = ChunkRows(rows, _settings.Csv.MaxRowsPerFile).ToList();

        Directory.CreateDirectory(ResolveCsvDirectory(table.Name, toDate));
        for (var i = 0; i < chunks.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var filePath = ResolveCsvPath(table.Name, fromDate, toDate, i + 1);
            await using var writer = new StreamWriter(filePath, append: false, encoding: new UTF8Encoding(false));
            await writer.WriteLineAsync(string.Join(delimiter, columns));

            foreach (var row in chunks[i])
            {
                var values = columns.Select(column => EscapeCsv(((IDictionary<string, object?>)row)[column], delimiter));
                await writer.WriteLineAsync(string.Join(delimiter, values));
            }
        }
    }

    /// <summary>
    /// 計算批次資料的日期範圍，方便命名檔案。
    /// </summary>
    /// <param name="rows">資料列。</param>
    /// <param name="dateColumn">日期欄位。</param>
    private (DateTime FromDate, DateTime ToDate) CalculateRange(IReadOnlyList<dynamic> rows, string dateColumn)
    {
        var dates = rows.Select(r => Convert.ToDateTime(((IDictionary<string, object?>)r)[dateColumn], CultureInfo.InvariantCulture)).ToList();
        return (dates.Min(), dates.Max());
    }

    /// <summary>
    /// 將資料列依大小分塊，避免一次載入過多記憶體。
    /// </summary>
    /// <param name="rows">來源資料。</param>
    /// <param name="size">每塊大小。</param>
    private IEnumerable<IReadOnlyList<dynamic>> ChunkRows(IReadOnlyList<dynamic> rows, int size)
    {
        for (var i = 0; i < rows.Count; i += size)
        {
            yield return rows.Skip(i).Take(size).ToList();
        }
    }

    /// <summary>
    /// 依照表名與年月解析 CSV 儲存目錄。
    /// </summary>
    private string ResolveCsvDirectory(string tableName, DateTime targetDate)
    {
        var folder = Path.Combine(_settings.Csv.RootFolder, tableName, targetDate.ToString("yyyyMM"));
        Directory.CreateDirectory(folder);
        return folder;
    }

    /// <summary>
    /// 建立符合命名規則的 CSV 完整路徑。
    /// </summary>
    private string ResolveCsvPath(string tableName, DateTime fromDate, DateTime toDate, int partIndex)
    {
        var directory = ResolveCsvDirectory(tableName, toDate);
        var fileName = _settings.Csv.FileNameFormat
            .Replace("{TableName}", tableName)
            .Replace("{FromDate:yyyyMMdd}", fromDate.ToString("yyyyMMdd"))
            .Replace("{ToDate:yyyyMMdd}", toDate.ToString("yyyyMMdd"))
            .Replace("{PartIndex}", partIndex.ToString("D2"));

        return Path.Combine(directory, fileName);
    }

    private static string EscapeCsv(object? value, string delimiter)
    {
        if (value is null)
        {
            return string.Empty;
        }

        var text = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        var needsQuote = text.Contains(delimiter, StringComparison.Ordinal) || text.Contains('"') || text.Contains('\n');

        if (needsQuote)
        {
            text = "\"" + text.Replace("\"", "\"\"") + "\"";
        }

        return text;
    }

    private static string Bracket(string value) => $"[{value}]";
}

/// <summary>
/// 合併預設值後的資料表設定，提供執行期使用。
/// </summary>
internal class EffectiveTableSettings
{
    public required string Name { get; init; }
    public required string SourceConnectionName { get; init; }
    public required string TargetConnectionName { get; init; }
    public required string DateColumn { get; init; }
    public required string PrimaryKeyColumn { get; init; }
    public required int OnlineRetentionMonths { get; init; }
    public required int HistoryRetentionMonths { get; init; }
    public required int BatchSize { get; init; }
    public required bool CsvEnabled { get; init; }
}
