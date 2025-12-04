using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Dapper;
using DataLifecycleManager.Configuration;
using DataLifecycleManager.Infrastructure;
using DataLifecycleManager.Models;
using DataLifecycleManager.Repositories;
using Microsoft.Data.SqlClient;
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
    /// SQL Server 單次 Command 最多 2100 個參數，這裡保守抓 1000。
    /// 所有使用 IN @Ids 的地方，都必須用這個常數做拆批。
    /// </summary>
    private const int MaxSqlParametersPerCommand = 1000;

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

        try
        {
            var settings = await _settingRepository.GetAllAsync(cancellationToken);
            var enabledSettings = settings.Where(s => s.Enabled).ToList();

            if (!enabledSettings.Any())
            {
                messages.Add("沒有啟用的搬移設定，未執行搬移流程。");
                return new MigrationResult(true, messages);
            }

            foreach (var setting in enabledSettings)
            {
                var cutoffOnline = DateTime.Now.AddMonths(-setting.OnlineRetentionMonths);
                var cutoffHistory = DateTime.Now.AddMonths(-setting.HistoryRetentionMonths);

                _logger.LogInformation(
                    "準備搬移 {Table}，線上截止 {OnlineCutoff:yyyy-MM-dd}，歷史截止 {HistoryCutoff:yyyy-MM-dd}",
                    setting.TableName, cutoffOnline, cutoffHistory);

                try
                {
                    // 線上搬到歷史庫
                    await _retryPolicyExecutor.ExecuteAsync(
                        $"{setting.TableName}-Archive",
                        () => ArchiveOnlineAsync(setting, cutoffOnline, cancellationToken),
                        cancellationToken);

                    _logger.LogInformation(
                        "-- 線上搬到歷史庫完成 ---------------------------------------------------------------------------------");
                    
                    // 歷史庫匯出 CSV + 刪除
                    if (setting.CsvEnabled)
                    {
                        await _retryPolicyExecutor.ExecuteAsync(
                            $"{setting.TableName}-Csv",
                            () => ExportHistoryAsync(setting, cutoffHistory, cancellationToken),
                            cancellationToken);
                    }

                    messages.Add(
                        $"{setting.TableName} 搬移完畢（線上>{cutoffOnline:yyyy-MM-dd}；歷史>{cutoffHistory:yyyy-MM-dd}）");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "搬移 {Table} 時發生錯誤", setting.TableName);
                    messages.Add($"[{setting.TableName}] 發生錯誤：{ex.GetBaseException().Message}");
                    return new MigrationResult(false, messages);
                }
            }

            return new MigrationResult(true, messages);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "搬移流程發生未處理例外");
            messages.Add($"搬移流程失敗：{ex.GetBaseException().Message}");
            return new MigrationResult(false, messages);
        }
    }

    #region 批次主流程（共用骨架）

    /// <summary>
    /// 線上庫 → 歷史庫（只做搬移，不刪歷史）。
    /// </summary>
    private Task ArchiveOnlineAsync(
        ArchiveSetting setting,
        DateTime cutoff,
        CancellationToken cancellationToken)
    {
        var batchOptions = CreateBatchExecutionOptions(setting);

        return ProcessBatchesAsync(
            setting,
            setting.SourceConnectionName,
            cutoff,
            batchOptions,
            emptyMessageTemplate: "{Table} 沒有需要搬移的資料。",
            handleBatchAsync: MoveBatchAsync,
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// 歷史庫 → CSV 匯出 + 刪除歷史資料。
    /// </summary>
    private Task ExportHistoryAsync(
        ArchiveSetting setting,
        DateTime cutoff,
        CancellationToken cancellationToken)
    {
        var batchOptions = CreateBatchExecutionOptions(setting);

        return ProcessBatchesAsync(
            setting,
            setting.TargetConnectionName,
            cutoff,
            batchOptions,
            emptyMessageTemplate: "{Table} 沒有需要匯出的歷史資料。",
            handleBatchAsync: async (s, rows, ct) =>
            {
                var (fromDate, toDate) = CalculateRange(rows, s.DateColumn);
                await WriteCsvFilesAsync(rows, s, fromDate, toDate, ct);
                await DeleteBatchAsync(s.TargetConnectionName, s.TableName, s.PrimaryKeyColumn, rows, ct);
            },
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// 共用的「批次處理」流程骨架：Fetch → Handle → 調整 BatchSize。
    /// </summary>
    private async Task ProcessBatchesAsync(
        ArchiveSetting setting,
        string connectionName,
        DateTime cutoff,
        BatchExecutionOptions options,
        string emptyMessageTemplate,
        Func<ArchiveSetting, IReadOnlyList<IDictionary<string, object?>>, CancellationToken, Task> handleBatchAsync,
        CancellationToken cancellationToken)
    {
        var currentBatchSize = options.InitialBatchSize;

        while (!cancellationToken.IsCancellationRequested)
        {
            var rows = await FetchBatchAsync(
                connectionName,
                setting.TableName,
                setting.DateColumn,
                cutoff,
                currentBatchSize,
                cancellationToken);

            if (!rows.Any())
            {
                _logger.LogInformation(emptyMessageTemplate, setting.TableName);
                break;
            }

            var sw = Stopwatch.StartNew();
            await handleBatchAsync(setting, rows, cancellationToken);
            sw.Stop();

            currentBatchSize = AdjustBatchSize(
                currentBatchSize,
                rows.Count,
                sw.Elapsed,
                options.MinBatchSize,
                options.MaxBatchSize,
                options.TargetBatchSeconds,
                setting.TableName);
        }
    }

    /// <summary>
    /// 依設定建立批次執行參數（之後要改 min/max/target 只要改這裡）。
    /// </summary>
    private static BatchExecutionOptions CreateBatchExecutionOptions(ArchiveSetting setting)
    {
        var initialBatchSize = setting.BatchSize > 0 ? setting.BatchSize : 1000;

        return new BatchExecutionOptions(
            InitialBatchSize: initialBatchSize,
            MinBatchSize: 100,
            MaxBatchSize: 2000,     // 保守一些，搭配 2100 參數上限比較安全
            TargetBatchSeconds: 20);
    }

    #endregion

    #region DB 查詢 / 搬移 / 刪除

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

        var command = new CommandDefinition(
            sql,
            new { BatchSize = batchSize, Cutoff = cutoff },
            cancellationToken: cancellationToken);

        var rows = await connection.QueryAsync(command);
        return rows.Select(r => (IDictionary<string, object?>)r).ToList();
    }

    /// <summary>
    /// 單批：線上庫 → 歷史庫，BulkInsert + 刪除來源。
    /// </summary>
    private async Task MoveBatchAsync(
        ArchiveSetting setting,
        IReadOnlyList<IDictionary<string, object?>> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        var columns = rows[0].Keys.ToList();

        // 取出這批的主鍵清單（來源庫）
        var primaryKeys = rows.Select(row =>
        {
            if (!row.TryGetValue(setting.PrimaryKeyColumn, out var key))
            {
                throw new InvalidOperationException(
                    $"搬移 {setting.TableName} 時缺少主鍵欄位 {setting.PrimaryKeyColumn}");
            }

            return key!;
        }).ToList();

        const int archiveCommandTimeout = 180;

        // 1. 寫到歷史庫（內部會先查歷史庫已存在 PK，避免 PK 衝突）
        await BulkInsertAsync(setting, rows, cancellationToken);

        // 2. 刪掉來源庫的這批資料（走拆批邏輯，避免 2100 參數）
        await DeleteByPrimaryKeysAsync(
            setting.SourceConnectionName,
            setting.TableName,
            setting.PrimaryKeyColumn,
            primaryKeys,
            archiveCommandTimeout,
            cancellationToken);

        _logger.LogInformation("{Table} 搬移 {Count} 筆完成。", setting.TableName, rows.Count);
    }

    /// <summary>
    /// 歷史庫依主鍵批次刪除資料，內部也會拆批避免 2100 參數上限。
    /// </summary>
    private async Task DeleteBatchAsync(
        string connectionName,
        string tableName,
        string primaryKeyColumn,
        IReadOnlyList<IDictionary<string, object?>> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        var primaryKeys = rows.Select(row =>
        {
            if (!row.TryGetValue(primaryKeyColumn, out var key))
            {
                throw new InvalidOperationException(
                    $"刪除 {tableName} 時找不到主鍵欄位 {primaryKeyColumn}");
            }

            if (key is null)
            {
                throw new InvalidOperationException(
                    $"刪除 {tableName} 時主鍵欄位 {primaryKeyColumn} 為空值");
            }

            return (object)key;
        }).ToList();

        await DeleteByPrimaryKeysAsync(
            connectionName,
            tableName,
            primaryKeyColumn,
            primaryKeys,
            commandTimeout: null,
            cancellationToken: cancellationToken);

        _logger.LogInformation(
            "{Table} 刪除 {Count} 筆歷史資料。",
            tableName,
            rows.Count);
    }

    /// <summary>
    /// 依主鍵清單，分批執行 DELETE，避免 IN @Ids 超過 2100 個參數。
    /// </summary>
    private async Task DeleteByPrimaryKeysAsync(
        string connectionName,
        string tableName,
        string primaryKeyColumn,
        IReadOnlyList<object> primaryKeys,
        int? commandTimeout,
        CancellationToken cancellationToken)
    {
        if (primaryKeys.Count == 0)
        {
            return;
        }

        var deleteSql = DynamicSqlHelper.BuildDeleteSql(tableName, primaryKeyColumn);

        await using var connection = _connectionFactory.CreateConnection(connectionName);
        await connection.OpenAsync(cancellationToken);

        for (var offset = 0; offset < primaryKeys.Count; offset += MaxSqlParametersPerCommand)
        {
            var batch = primaryKeys
                .Skip(offset)
                .Take(MaxSqlParametersPerCommand)
                .ToList();

            var cmd = new CommandDefinition(
                deleteSql,
                new { Ids = batch },
                commandTimeout: commandTimeout,
                cancellationToken: cancellationToken);

            await connection.ExecuteAsync(cmd);
        }

        _logger.LogInformation(
            "{Table} 依主鍵分批刪除完成，共刪除 {Count} 筆資料。",
            tableName,
            primaryKeys.Count);
    }

    #endregion

    #region CSV 匯出相關

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
        var folder = Path.Combine(
            setting.CsvRootFolder,
            setting.TableName,
            targetDate.ToString("yyyyMM"));

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
        var needsQuote = text.Contains(delimiter, StringComparison.Ordinal)
                         || text.Contains('"')
                         || text.Contains('\n');

        if (needsQuote)
        {
            text = "\"" + text.Replace("\"", "\"\"") + "\"";
        }

        return text;
    }

    #endregion

    #region BatchSize 自適應調整

    /// <summary>
    /// 根據單批實際耗時與筆數，自適應調整下一批的 BatchSize。
    /// </summary>
    private int AdjustBatchSize(
        int currentBatchSize,
        int actualRowCount,
        TimeSpan elapsed,
        int minBatchSize,
        int maxBatchSize,
        int targetBatchSeconds,
        string tableName)
    {
        if (actualRowCount <= 0)
        {
            return currentBatchSize;
        }

        var elapsedSeconds = elapsed.TotalSeconds;

        // 太慢：> 1.5 * target，直接對半砍（但不低於 min）
        if (elapsedSeconds > targetBatchSeconds * 1.5)
        {
            var newSize = Math.Max(minBatchSize, currentBatchSize / 2);
            if (newSize < currentBatchSize)
            {
                _logger.LogInformation(
                    "{Table} 單批耗時 {Elapsed:F1}s，將 BatchSize 由 {Old} 調降為 {New}",
                    tableName, elapsedSeconds, currentBatchSize, newSize);
                return newSize;
            }

            return currentBatchSize;
        }

        // 很快而且有跑滿：< target / 2 且實際筆數 >= currentBatchSize，往上加倍（但不超過 max）
        if (elapsedSeconds < targetBatchSeconds / 2.0 && actualRowCount >= currentBatchSize)
        {
            var newSize = Math.Min(maxBatchSize, currentBatchSize * 2);
            if (newSize > currentBatchSize)
            {
                _logger.LogInformation(
                    "{Table} 單批耗時 {Elapsed:F1}s 且跑滿 {Count} 筆，將 BatchSize 由 {Old} 調升為 {New}",
                    tableName, elapsedSeconds, actualRowCount, currentBatchSize, newSize);
                return newSize;
            }

            return currentBatchSize;
        }

        // 其他情況維持現有大小
        return currentBatchSize;
    }

    #endregion

    #region BulkInsert（歷史庫）

    /// <summary>
    /// 使用 SqlBulkCopy 將單批資料匯入目標表，
    /// 並在匯入前先過濾掉「歷史表中已存在主鍵」的資料，避免重複寫入。
    /// 主鍵比較統一轉成字串，並將 IN 參數拆批，避免超過 SQL Server 2100 參數上限。
    /// </summary>
    private async Task BulkInsertAsync(
        ArchiveSetting setting,
        IReadOnlyList<IDictionary<string, object?>> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        var columns = rows[0].Keys.ToList();

        if (!columns.Contains(setting.PrimaryKeyColumn))
        {
            throw new InvalidOperationException(
                $"BulkInsert {setting.TableName} 時找不到主鍵欄位 {setting.PrimaryKeyColumn}");
        }

        // 1. 先整理這批要搬的主鍵清單（全部轉成字串）
        var primaryKeyStrings = rows.Select(row =>
        {
            if (!row.TryGetValue(setting.PrimaryKeyColumn, out var key) || key is null)
            {
                throw new InvalidOperationException(
                    $"BulkInsert {setting.TableName} 時主鍵欄位 {setting.PrimaryKeyColumn} 為空值或不存在");
            }

            var text = Convert.ToString(key, CultureInfo.InvariantCulture);
            if (string.IsNullOrWhiteSpace(text))
            {
                throw new InvalidOperationException(
                    $"BulkInsert {setting.TableName} 時主鍵欄位 {setting.PrimaryKeyColumn} 轉字串後為空值。");
            }

            return text;
        }).ToList();

        await using var connection = _connectionFactory.CreateConnection(setting.TargetConnectionName);
        await connection.OpenAsync(cancellationToken);

        // 2. 查出「歷史表中已存在」的主鍵（拆批避免超過 2100 個參數）
        var existingIdSql = $"""
                             SELECT CAST([{setting.PrimaryKeyColumn}] AS NVARCHAR(50)) AS IdText
                             FROM [{setting.TableName}]
                             WHERE [{setting.PrimaryKeyColumn}] IN @Ids;
                             """;

        var existingIds = new HashSet<string>(StringComparer.Ordinal);

        for (var offset = 0; offset < primaryKeyStrings.Count; offset += MaxSqlParametersPerCommand)
        {
            var batchIds = primaryKeyStrings
                .Skip(offset)
                .Take(MaxSqlParametersPerCommand)
                .ToList();

            var batchResult = await connection.QueryAsync<string>(
                new CommandDefinition(
                    existingIdSql,
                    new { Ids = batchIds },
                    cancellationToken: cancellationToken));

            foreach (var id in batchResult)
            {
                existingIds.Add(id);
            }
        }

        // 3. 過濾掉已存在的 PK，只保留真正要匯入的列
        var filteredRows = rows
            .Where(row =>
            {
                row.TryGetValue(setting.PrimaryKeyColumn, out var key);
                if (key is null)
                {
                    return false;
                }

                var text = Convert.ToString(key, CultureInfo.InvariantCulture);
                return !string.IsNullOrWhiteSpace(text) && !existingIds.Contains(text);
            })
            .ToList();

        if (filteredRows.Count == 0)
        {
            _logger.LogInformation(
                "{Table} 此批 {Total} 筆資料在歷史表皆已存在，略過 BulkInsert。",
                setting.TableName,
                rows.Count);
            return;
        }

        _logger.LogInformation(
            "{Table} 此批 {Total} 筆資料中，有 {Skipped} 筆已存在歷史表，實際匯入 {Inserted} 筆。",
            setting.TableName,
            rows.Count,
            rows.Count - filteredRows.Count,
            filteredRows.Count);

        // 4. 建 DataTable schema（用所有欄位，跟原本一樣）
        var table = new DataTable();
        foreach (var column in columns)
        {
            table.Columns.Add(column, typeof(object));
        }

        // 5. 只把「未存在」的列塞進 DataTable
        foreach (var row in filteredRows)
        {
            var dataRow = table.NewRow();
            foreach (var column in columns)
            {
                row.TryGetValue(column, out var value);
                dataRow[column] = value ?? DBNull.Value;
            }
            table.Rows.Add(dataRow);
        }

        // 6. 用 SqlBulkCopy 寫進去（沿用同一個 connection）
        if (connection is not SqlConnection sqlConnection)
        {
            throw new InvalidOperationException("BulkInsert 只支援 SqlConnection。");
        }

        using var bulkCopy = new SqlBulkCopy(sqlConnection)
        {
            DestinationTableName = setting.TableName,
            BulkCopyTimeout = 180,
            BatchSize = filteredRows.Count
        };

        foreach (var column in columns)
        {
            bulkCopy.ColumnMappings.Add(column, column);
        }

        await bulkCopy.WriteToServerAsync(table, cancellationToken);
    }

    #endregion

    /// <summary>
    /// 批次執行的共用設定。
    /// </summary>
    private sealed record BatchExecutionOptions(
        int InitialBatchSize,
        int MinBatchSize,
        int MaxBatchSize,
        int TargetBatchSeconds);
}

/// <summary>
/// 單次搬移的執行結果封裝。
/// </summary>
public record MigrationResult(bool Succeeded, IReadOnlyList<string> Messages);
