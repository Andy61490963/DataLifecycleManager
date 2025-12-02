using System.Dynamic;
using System.Globalization;
using System.Text;
using Dapper;
using DataLifecycleManager.Configuration;
using DataLifecycleManager.Domain;
using DataLifecycleManager.Infrastructure;
using Microsoft.Extensions.Logging;

namespace DataLifecycleManager.Services;

/// <summary>
/// 整合線上庫搬移與 CSV 歷史匯出的流程控制器。
/// </summary>
public class ArchiveCoordinator
{
    private readonly ArchiveDefaultsOptions _defaults;
    private readonly IArchiveSettingsProvider _settingsProvider;
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
    public ArchiveCoordinator(
        IArchiveSettingsProvider settingsProvider,
        Microsoft.Extensions.Options.IOptions<ArchiveDefaultsOptions> defaults,
        SqlConnectionFactory connectionFactory,
        RetryPolicyExecutor retryExecutor,
        ILogger<ArchiveCoordinator> logger)
    {
        _settingsProvider = settingsProvider;
        _defaults = defaults.Value;
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
        if (!_defaults.Enabled)
        {
            _logger.LogInformation("歸檔排程已停用，略過本次執行。");
            return;
        }

        var tables = await _settingsProvider.GetJobsAsync(cancellationToken);

        foreach (var table in tables)
        {
            await _retryExecutor.ExecuteAsync(
                $"{table.TableName}-Archive",
                table.RetryPolicy,
                () => ArchiveOnlineAsync(table, cancellationToken),
                cancellationToken);

            if (table.CsvEnabled && _defaults.Csv.Enabled)
            {
                await _retryExecutor.ExecuteAsync(
                    $"{table.TableName}-Csv",
                    table.RetryPolicy,
                    () => ExportHistoryAsync(table, cancellationToken),
                    cancellationToken);
            }
        }
    }

    /// <summary>
    /// 針對單一資料表搬移超過保留期的線上資料到歷史庫。
    /// </summary>
    /// <param name="table">合併後的表設定。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    private async Task ArchiveOnlineAsync(ArchiveJobRuntimeSettings table, CancellationToken cancellationToken)
    {
        // 以月為層級，產生斷點
        var cutoff = DateTime.Now.AddMonths(-table.OnlineRetentionMonths);
        _logger.LogInformation("開始搬移 {Table}，截止日 {Cutoff:yyyy-MM-dd}，批次 {BatchSize}", table.TableName, cutoff, table.BatchSize);

        // 如果使用者沒有手動取消
        while (!cancellationToken.IsCancellationRequested)
        {
            var rows = await FetchBatchAsync(table.SourceConnection, table.TableName, table.DateColumn, cutoff, table.BatchSize, cancellationToken);

            // 根據 BatchSize 一直切片，直到都取完為止
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
    private async Task ExportHistoryAsync(ArchiveJobRuntimeSettings table, CancellationToken cancellationToken)
    {
        var cutoff = DateTime.Now.AddMonths(-table.HistoryRetentionMonths);
        _logger.LogInformation("開始 CSV 匯出 {Table}，截止日 {Cutoff:yyyy-MM-dd}，批次 {BatchSize}", table.TableName, cutoff, table.BatchSize);

        while (!cancellationToken.IsCancellationRequested)
        {
            var rows = await FetchBatchAsync(table.TargetConnection, table.TableName, table.DateColumn, cutoff, table.BatchSize, cancellationToken);
            if (!rows.Any())
            {
                break;
            }

            var (fromDate, toDate) = CalculateRange(rows, table.DateColumn);
            await WriteCsvFilesAsync(rows, table, fromDate, toDate, cancellationToken);
            await DeleteBatchAsync(table.TargetConnection, table.TableName, table.PrimaryKeyColumn, rows, cancellationToken);
        }
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
    private async Task<IReadOnlyList<IDictionary<string, object?>>> FetchBatchAsync(
        string connectionName,
        string tableName,
        string dateColumn,
        DateTime cutoff,
        int batchSize,
        CancellationToken cancellationToken)
    {
        var sql = $"""
                   SELECT TOP (@BatchSize) *
                   FROM {Bracket(tableName)} WITH (READPAST)
                   WHERE {Bracket(dateColumn)} < @Cutoff
                   ORDER BY {Bracket(dateColumn)} ASC;
                   """;
        
        await using var connection = _connectionFactory.CreateConnection(connectionName);
        await connection.OpenAsync(cancellationToken);

        // Dapper 預設會回傳 dynamic Row
        var rows = await connection.QueryAsync(sql, new { BatchSize = batchSize, Cutoff = cutoff });

        // 型別投影，以 IDictionary 介面操作 key = 欄位名稱, value = 欄位內容
        return rows
            .Select(r => (IDictionary<string, object?>)r)
            .ToList();
    }

    /// <summary>
    /// 使用 TransactionScope 將單批資料搬移至目標庫並刪除來源。
    /// </summary>
    /// <param name="table">合併後的設定。</param>
    /// <param name="rows">待搬移資料。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    /// <summary>
    /// 使用 TransactionScope 將單批資料搬移至目標庫並刪除來源。
    /// </summary>
    private async Task MoveBatchAsync(
        ArchiveJobRuntimeSettings table,
        IReadOnlyList<IDictionary<string, object?>> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            _logger.LogInformation("{Table} 無資料可搬移。", table.TableName);
            return;
        }

        // 1. 從第一筆資料的 key 當欄位清單
        var columns = rows[0].Keys.ToList();

        // 2. 組 Insert / Delete SQL
        var insertSql = DynamicSqlHelper.BuildInsertSql(table.TableName, columns, table.PrimaryKeyColumn);
        var deleteSql = DynamicSqlHelper.BuildDeleteSql(table.TableName, table.PrimaryKeyColumn);

        // 3. 主鍵集合
        var primaryKeys = rows
            .Select(row =>
            {
                if (!row.TryGetValue(table.PrimaryKeyColumn, out var key))
                {
                    throw new InvalidOperationException(
                        $"搬移 {table.TableName} 時找不到主鍵欄位 {table.PrimaryKeyColumn}。");
                }
                return key;
            })
            .ToList();

        // 1. 先寫入歷史庫（DB2）
        await using (var target = _connectionFactory.CreateConnection(table.TargetConnection))
        {
            await target.OpenAsync(cancellationToken);

            var parameterBag = rows.Select(row =>
            {
                var dp = new DynamicParameters();

                foreach (var (columnName, value) in row)
                {
                    dp.Add(columnName, value);
                }

                return (object)dp; // 讓外層變成 IEnumerable<object>
            });

            await target.ExecuteAsync(insertSql, parameterBag);
        }

        // 2. 再刪除線上庫（DB1）
        await using (var source = _connectionFactory.CreateConnection(table.SourceConnection))
        {
            await source.OpenAsync(cancellationToken);

            // 這裡的 List<object?> 給 IN @Ids 用，是 Dapper 支援的 pattern
            await source.ExecuteAsync(deleteSql, new { Ids = primaryKeys });
        }
        
        _logger.LogInformation("{Table} 搬移 {Count} 筆完成。", table.TableName, rows.Count);
    }
    
    /// <summary>
    /// 以主鍵批次刪除指定資料。
    /// </summary>
    /// <param name="connectionName">連線名稱。</param>
    /// <param name="tableName">資料表。</param>
    /// <param name="primaryKeyColumn">主鍵欄位。</param>
    /// <param name="rows">目標資料列。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    /// <summary>
    /// 以主鍵批次刪除指定資料。
    /// </summary>
    private async Task DeleteBatchAsync(
        string connectionName,
        string tableName,
        string primaryKeyColumn,
        IReadOnlyList<IDictionary<string, object?>> rows,
        CancellationToken cancellationToken)
    {
        var deleteSql = DynamicSqlHelper.BuildDeleteSql(tableName, primaryKeyColumn);

        var primaryKeys = rows
            .Select(row =>
            {
                if (!row.TryGetValue(primaryKeyColumn, out var key))
                {
                    throw new InvalidOperationException(
                        $"刪除 {tableName} 時找不到主鍵欄位 {primaryKeyColumn}。");
                }
                return key;
            })
            .ToList();

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
    /// <summary>
    /// 將批次資料依分段規則輸出為 CSV 檔案。
    /// </summary>
    private async Task WriteCsvFilesAsync(
        IReadOnlyList<IDictionary<string, object?>> rows,
        ArchiveJobRuntimeSettings table,
        DateTime fromDate,
        DateTime toDate,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        var columns   = rows[0].Keys.ToList();
        var delimiter = _defaults.Csv.Delimiter;
        var chunks    = ChunkRows(rows, _defaults.Csv.MaxRowsPerFile).ToList();

        Directory.CreateDirectory(ResolveCsvDirectory(table.TableName, table.CsvRootFolder, toDate));

        for (var i = 0; i < chunks.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var filePath = ResolveCsvPath(table.TableName, table.CsvRootFolder, fromDate, toDate, i + 1);
            await using var writer = new StreamWriter(filePath, append: false, encoding: new UTF8Encoding(true));

            // header
            await writer.WriteLineAsync(string.Join(delimiter, columns));

            // rows
            foreach (var row in chunks[i])
            {
                var values = columns.Select(column =>
                {
                    row.TryGetValue(column, out var value);
                    return EscapeCsv(value, delimiter);
                });

                await writer.WriteLineAsync(string.Join(delimiter, values));
            }
        }
    }


    /// <summary>
    /// 計算批次資料的日期範圍，方便命名檔案。
    /// </summary>
    /// <param name="rows">資料列。</param>
    /// <param name="dateColumn">日期欄位。</param>
    /// <summary>
    /// 計算批次資料的日期範圍，方便命名檔案。
    /// </summary>
    private (DateTime FromDate, DateTime ToDate) CalculateRange(
        IReadOnlyList<IDictionary<string, object?>> rows,
        string dateColumn)
    {
        var dates = rows
            .Select(row =>
            {
                if (!row.TryGetValue(dateColumn, out var value))
                {
                    throw new InvalidOperationException(
                        $"無法從資料列取得日期欄位 {dateColumn}。");
                }

                return Convert.ToDateTime(value, CultureInfo.InvariantCulture);
            })
            .ToList();

        return (dates.Min(), dates.Max());
    }

    /// <summary>
    /// 將資料列依大小分塊，避免一次載入過多記憶體。
    /// </summary>
    /// <param name="rows">來源資料。</param>
    /// <param name="size">每塊大小。</param>
    private static IEnumerable<IReadOnlyList<IDictionary<string, object?>>> ChunkRows(
        IReadOnlyList<IDictionary<string, object?>> rows,
        int size)
    {
        for (var i = 0; i < rows.Count; i += size)
        {
            yield return rows.Skip(i).Take(size).ToList();
        }
    }

    /// <summary>
    /// 依照表名與年月解析 CSV 儲存目錄。
    /// </summary>
    private string ResolveCsvDirectory(string tableName, string rootFolder, DateTime targetDate)
    {
        var folder = Path.Combine(rootFolder, tableName, targetDate.ToString("yyyyMM"));
        Directory.CreateDirectory(folder);
        return folder;
    }

    /// <summary>
    /// 建立符合命名規則的 CSV 完整路徑。
    /// </summary>
    private string ResolveCsvPath(string tableName, string rootFolder, DateTime fromDate, DateTime toDate, int partIndex)
    {
        var directory = ResolveCsvDirectory(tableName, rootFolder, toDate);
        var fileName = _defaults.Csv.FileNameFormat
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

