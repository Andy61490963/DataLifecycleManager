using System.Globalization;
using System.IO;
using System.Text;
using Dapper;
using DataLifecycleManager.Configuration;
using DataLifecycleManager.Infrastructure;
using DataLifecycleManager.Models;
using DataLifecycleManager.Repositories;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DataLifecycleManager.Services;

/// <summary>
/// 提供「開始搬移」的同步流程，包含查詢、搬移、匯出與刪除邏輯。
/// </summary>
public class ArchiveExecutionService
{
    private readonly IArchiveSettingRepository _settingRepository;
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly RetryPolicyExecutor _retryPolicyExecutor;
    private readonly CsvOptions _csvOptions;
    private readonly ILogger<ArchiveExecutionService> _logger;

    /// <summary>
    /// 建構子注入 Repository、連線工廠與共用設定。
    /// </summary>
    public ArchiveExecutionService(
        IArchiveSettingRepository settingRepository,
        SqlConnectionFactory connectionFactory,
        RetryPolicyExecutor retryPolicyExecutor,
        IOptions<CsvOptions> csvOptions,
        ILogger<ArchiveExecutionService> logger)
    {
        _settingRepository = settingRepository;
        _connectionFactory = connectionFactory;
        _retryPolicyExecutor = retryPolicyExecutor;
        _csvOptions = csvOptions.Value;
        _logger = logger;
    }

    /// <summary>
    /// 讀取所有設定並同步執行一次搬移流程。
    /// </summary>
    /// <param name="cancellationToken">取消權杖。</param>
    /// <returns>搬移結果與訊息。</returns>
    public async Task<MigrationResult> RunOnceAsync(CancellationToken cancellationToken)
    {
        var messages = new List<string>();
        var settings = await _settingRepository.GetAllAsync(cancellationToken);

        foreach (var setting in settings)
        {
            var cutoffOnline = DateTime.Now.AddMonths(-setting.OnlineRetentionMonths);
            var cutoffHistory = DateTime.Now.AddMonths(-setting.HistoryRetentionMonths);

            _logger.LogInformation("準備搬移 {Table}，線上截止 {OnlineCutoff:yyyy-MM-dd}，歷史截止 {HistoryCutoff:yyyy-MM-dd}", setting.TableName, cutoffOnline, cutoffHistory);

            await _retryPolicyExecutor.ExecuteAsync(
                $"{setting.TableName}-Archive",
                () => ArchiveOnlineAsync(setting, cutoffOnline, cancellationToken),
                cancellationToken);

            if (setting.CsvEnabled)
            {
                await _retryPolicyExecutor.ExecuteAsync(
                    $"{setting.TableName}-Csv",
                    () => ExportHistoryAsync(setting, cutoffHistory, cancellationToken),
                    cancellationToken);
            }

            messages.Add($"{setting.TableName} 搬移完畢（線上>{cutoffOnline:yyyy-MM-dd}；歷史>{cutoffHistory:yyyy-MM-dd}）");
        }

        return new MigrationResult(true, messages);
    }

    /// <summary>
    /// 從來源資料庫抓取批次資料並搬移到目標資料庫。
    /// </summary>
    private async Task ArchiveOnlineAsync(ArchiveSetting setting, DateTime cutoff, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var rows = await FetchBatchAsync(setting.SourceConnectionName, setting.TableName, setting.DateColumn, cutoff, setting.BatchSize, cancellationToken);
            if (!rows.Any())
            {
                _logger.LogInformation("{Table} 沒有需要搬移的資料。", setting.TableName);
                break;
            }

            await MoveBatchAsync(setting, rows, cancellationToken);
        }
    }

    /// <summary>
    /// 對歷史資料執行匯出與刪除。
    /// </summary>
    private async Task ExportHistoryAsync(ArchiveSetting setting, DateTime cutoff, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var rows = await FetchBatchAsync(setting.TargetConnectionName, setting.TableName, setting.DateColumn, cutoff, setting.BatchSize, cancellationToken);
            if (!rows.Any())
            {
                _logger.LogInformation("{Table} 沒有需要匯出的歷史資料。", setting.TableName);
                break;
            }

            var (fromDate, toDate) = CalculateRange(rows, setting.DateColumn);
            await WriteCsvFilesAsync(rows, setting, fromDate, toDate, cancellationToken);
            await DeleteBatchAsync(setting.TargetConnectionName, setting.TableName, setting.PrimaryKeyColumn, rows, cancellationToken);
        }
    }

    /// <summary>
    /// 抓取符合條件的批次資料。
    /// </summary>
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
                   FROM [{tableName}] WITH (READPAST)
                   WHERE [{dateColumn}] < @Cutoff
                   ORDER BY [{dateColumn}] ASC;
                   """;

        await using var connection = _connectionFactory.CreateConnection(connectionName);
        await connection.OpenAsync(cancellationToken);

        var command = new CommandDefinition(sql, new { BatchSize = batchSize, Cutoff = cutoff }, cancellationToken: cancellationToken);
        var rows = await connection.QueryAsync(command);
        return rows.Select(r => (IDictionary<string, object?>)r).ToList();
    }

    /// <summary>
    /// 執行單批資料的 insert + delete，透過 NOT EXISTS 確保冪等。
    /// </summary>
    private async Task MoveBatchAsync(ArchiveSetting setting, IReadOnlyList<IDictionary<string, object?>> rows, CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        var columns = rows[0].Keys.ToList();
        var insertSql = DynamicSqlHelper.BuildInsertSql(setting.TableName, columns, setting.PrimaryKeyColumn);
        var deleteSql = DynamicSqlHelper.BuildDeleteSql(setting.TableName, setting.PrimaryKeyColumn);

        var primaryKeys = rows.Select(row =>
        {
            if (!row.TryGetValue(setting.PrimaryKeyColumn, out var key))
            {
                throw new InvalidOperationException($"搬移 {setting.TableName} 時缺少主鍵欄位 {setting.PrimaryKeyColumn}");
            }

            return key;
        }).ToList();

        await using (var target = _connectionFactory.CreateConnection(setting.TargetConnectionName))
        {
            await target.OpenAsync(cancellationToken);

            var parameterBag = rows.Select(row =>
            {
                var dp = new DynamicParameters();
                foreach (var (columnName, value) in row)
                {
                    dp.Add(columnName, value);
                }

                return (object)dp;
            });

            await target.ExecuteAsync(new CommandDefinition(insertSql, parameterBag, cancellationToken: cancellationToken));
        }

        await using (var source = _connectionFactory.CreateConnection(setting.SourceConnectionName))
        {
            await source.OpenAsync(cancellationToken);
            await source.ExecuteAsync(new CommandDefinition(deleteSql, new { Ids = primaryKeys }, cancellationToken: cancellationToken));
        }

        _logger.LogInformation("{Table} 搬移 {Count} 筆完成。", setting.TableName, rows.Count);
    }

    /// <summary>
    /// 依主鍵批次刪除資料。
    /// </summary>
    private async Task DeleteBatchAsync(
        string connectionName,
        string tableName,
        string primaryKeyColumn,
        IReadOnlyList<IDictionary<string, object?>> rows,
        CancellationToken cancellationToken)
    {
        var deleteSql = DynamicSqlHelper.BuildDeleteSql(tableName, primaryKeyColumn);

        var primaryKeys = rows.Select(row =>
        {
            if (!row.TryGetValue(primaryKeyColumn, out var key))
            {
                throw new InvalidOperationException($"刪除 {tableName} 時找不到主鍵欄位 {primaryKeyColumn}");
            }

            return key;
        }).ToList();

        await using var connection = _connectionFactory.CreateConnection(connectionName);
        await connection.OpenAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(deleteSql, new { Ids = primaryKeys }, cancellationToken: cancellationToken));

        _logger.LogInformation("{Table} 刪除 {Count} 筆歷史資料。", tableName, rows.Count);
    }

    /// <summary>
    /// 將資料列依設定輸出為 CSV 檔案。
    /// </summary>
    private async Task WriteCsvFilesAsync(
        IReadOnlyList<IDictionary<string, object?>> rows,
        ArchiveSetting setting,
        DateTime fromDate,
        DateTime toDate,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        var columns = rows[0].Keys.ToList();
        var chunks = ChunkRows(rows, _csvOptions.MaxRowsPerFile).ToList();

        Directory.CreateDirectory(ResolveCsvDirectory(setting, toDate));

        for (var i = 0; i < chunks.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var filePath = ResolveCsvPath(setting, fromDate, toDate, i + 1);
            await using var writer = new StreamWriter(filePath, append: false, encoding: new UTF8Encoding(true));

            await writer.WriteLineAsync(string.Join(_csvOptions.Delimiter, columns));

            foreach (var row in chunks[i])
            {
                var values = columns.Select(column =>
                {
                    row.TryGetValue(column, out var value);
                    return EscapeCsv(value, _csvOptions.Delimiter);
                });

                await writer.WriteLineAsync(string.Join(_csvOptions.Delimiter, values));
            }
        }
    }

    /// <summary>
    /// 計算批次資料的日期範圍，便於命名檔案。
    /// </summary>
    private (DateTime FromDate, DateTime ToDate) CalculateRange(
        IReadOnlyList<IDictionary<string, object?>> rows,
        string dateColumn)
    {
        var dates = rows.Select(row =>
        {
            if (!row.TryGetValue(dateColumn, out var value))
            {
                throw new InvalidOperationException($"無法從資料列取得日期欄位 {dateColumn}");
            }

            return Convert.ToDateTime(value, CultureInfo.InvariantCulture);
        }).ToList();

        return (dates.Min(), dates.Max());
    }

    private static IEnumerable<IReadOnlyList<IDictionary<string, object?>>> ChunkRows(
        IReadOnlyList<IDictionary<string, object?>> rows,
        int size)
    {
        for (var i = 0; i < rows.Count; i += size)
        {
            yield return rows.Skip(i).Take(size).ToList();
        }
    }

    private string ResolveCsvDirectory(ArchiveSetting setting, DateTime targetDate)
    {
        var folder = Path.Combine(setting.CsvRootFolder, setting.TableName, targetDate.ToString("yyyyMM"));
        Directory.CreateDirectory(folder);
        return folder;
    }

    private string ResolveCsvPath(ArchiveSetting setting, DateTime fromDate, DateTime toDate, int partIndex)
    {
        var directory = ResolveCsvDirectory(setting, toDate);
        var fileName = _csvOptions.FileNameFormat
            .Replace("{TableName}", setting.TableName)
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
}

/// <summary>
/// 單次搬移的執行結果封裝。
/// </summary>
public record MigrationResult(bool Succeeded, IReadOnlyList<string> Messages);
