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
/// 整體流程：
/// 1. 讀取每個 Table 的搬移設定（保留天數 / 批次大小 / 是否刪來源 / 是否輸出 CSV）
/// 2. 線上庫符合條件的資料批次搬到歷史庫（ArchiveOnline）
/// 3. 歷史庫符合更舊的資料批次匯出 CSV 並刪除（ExportHistory）
/// 4. 透過游標式批次查詢 + 自適應 BatchSize 避免無窮迴圈與效能炸掉
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
    /// 一次 Run：
    /// - 針對每個啟用的 ArchiveSetting：
    ///   1. 線上庫搬到歷史庫
    ///   2. 需要的話再做歷史庫匯出 + 刪除
    /// </summary>
    /// <param name="cancellationToken">取消權杖。</param>
    /// <returns>搬移結果與訊息。</returns>
    public async Task<MigrationResult> RunOnceAsync(CancellationToken cancellationToken)
    {
        var messages = new List<string>();

        try
        {
            // 讀取所有搬移設定
            var settings = await _settingRepository.GetAllAsync(cancellationToken);
            var enabledSettings = settings.Where(s => s.Enabled).ToList();

            if (!enabledSettings.Any())
            {
                messages.Add("沒有啟用的搬移設定，未執行搬移流程。");
                return new MigrationResult(true, messages);
            }

            // 一個 Table 設定一個搬移流程
            foreach (var setting in enabledSettings)
            {
                // 線上 / 歷史的 cutoff 日，只吃日期部分（去掉時間）
                var cutoffOnline = setting.OnlineRetentionDate.Date;
                var cutoffHistory = setting.HistoryRetentionDate.Date;

                // 安全檢查：線上保留一定要 > 歷史保留，否則設定有問題
                if (cutoffOnline <= cutoffHistory)
                {
                    _logger.LogWarning(
                        "設定 {Table} 的線上保留日期 {OnlineCutoff:yyyy-MM-dd} 不得早於或等於歷史保留日期 {HistoryCutoff:yyyy-MM-dd}，已跳過",
                        setting.TableName, cutoffOnline, cutoffHistory);
                    messages.Add($"{setting.TableName} 的線上 / 歷史保留日期設定有誤，未執行搬移。");
                    continue;
                }

                _logger.LogInformation(
                    "準備搬移 {Table}，線上截止 {OnlineCutoff:yyyy-MM-dd}，歷史截止 {HistoryCutoff:yyyy-MM-dd}",
                    setting.TableName, cutoffOnline, cutoffHistory);

                try
                {
                    // === Phase 1：線上搬到歷史庫 ===
                    // 透過 RetryPolicy 包起來，搬移失敗會走重試策略
                    await _retryPolicyExecutor.ExecuteAsync(
                        $"{setting.TableName}-Archive",
                        () => ArchiveOnlineAsync(setting, cutoffOnline, cancellationToken),
                        cancellationToken);

                    _logger.LogInformation(
                        "-- 線上搬到歷史庫完成 ---------------------------------------------------------------------------------");
                    
                    // === Phase 2：歷史庫匯出 CSV + 刪除歷史 ===
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
                    // 單一 Table 發生錯誤就當作這次 Run 失敗（直接 return）
                    _logger.LogError(ex, "搬移 {Table} 時發生錯誤", setting.TableName);
                    messages.Add($"[{setting.TableName}] 發生錯誤：{ex.GetBaseException().Message}");
                    return new MigrationResult(false, messages);
                }
            }

            return new MigrationResult(true, messages);
        }
        catch (Exception ex)
        {
            // 任何預期外例外都被包起來，避免炸掉整個 Host
            _logger.LogError(ex, "搬移流程發生未處理例外");
            messages.Add($"搬移流程失敗：{ex.GetBaseException().Message}");
            return new MigrationResult(false, messages);
        }
    }

    #region 批次主流程（共用骨架）

    /// <summary>
    /// 線上庫 → 歷史庫（只做搬移，不刪歷史）。
    /// 這裡只決定要用哪個 Connection + cutoff，實際批次邏輯在 ProcessBatchesAsync。
    /// </summary>
    private Task ArchiveOnlineAsync(
        ArchiveSetting setting,
        DateTime cutoff,
        CancellationToken cancellationToken)
    {
        var batchOptions = CreateBatchExecutionOptions(setting);

        return ProcessBatchesAsync(
            setting,
            setting.SourceConnectionName,   // 線上資料庫連線
            cutoff,
            batchOptions,
            emptyMessageTemplate: "{Table} 沒有需要搬移的資料。",
            handleBatchAsync: MoveBatchAsync,  // 單批處理委派給 MoveBatchAsync
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// 歷史庫 → CSV 匯出 + 刪除歷史資料。
    /// 同樣走共用批次流程，只是 handleBatch 的邏輯換成「寫檔 + 刪除歷史」。
    /// </summary>
    private Task ExportHistoryAsync(
        ArchiveSetting setting,
        DateTime cutoff,
        CancellationToken cancellationToken)
    {
        var batchOptions = CreateBatchExecutionOptions(setting);

        return ProcessBatchesAsync(
            setting,
            setting.TargetConnectionName,   // 歷史資料庫連線
            cutoff,
            batchOptions,
            emptyMessageTemplate: "{Table} 沒有需要匯出的歷史資料。",
            handleBatchAsync: async (s, rows, ct) =>
            {
                // 依這批資料算出日期範圍，給 CSV 檔名用
                var (fromDate, toDate) = CalculateRange(rows, s.DateColumn);

                // 把這批資料寫成一個或多個 CSV 檔
                await WriteCsvFilesAsync(rows, s, fromDate, toDate, ct);

                // 寫檔成功後，刪掉這批歷史資料（依 PK 批次刪除）
                await DeleteBatchAsync(s.TargetConnectionName, s.TableName, s.PrimaryKeyColumn, rows, ct);
            },
            cancellationToken: cancellationToken);
    }

    /// <summary>
    /// 共用的「批次處理」流程骨架：Fetch → Handle → 調整 BatchSize。
    /// 這裡是最重要的 while 迴圈：
    /// - 使用 (lastDate, lastPrimaryKey) 作為游標，確保每批都往後推進，避免無窮迴圈。
    /// - 使用 AdjustBatchSize 根據耗時自動調整 BatchSize，避免一次塞太多或太小。
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

        // 游標狀態：上一批「最後一筆」的日期與主鍵
        // 第一輪為 null → 從最舊開始抓；後續每一輪只抓比這個游標更後面的資料
        DateTime? lastDate = null;
        object? lastPrimaryKey = null;
        
        while (!cancellationToken.IsCancellationRequested)
        {
            // 1. 依游標抓一批資料
            var rows = await FetchBatchAsync(
                connectionName,
                setting.TableName,
                setting.DateColumn,
                setting.PrimaryKeyColumn,
                cutoff,
                currentBatchSize,
                lastDate,
                lastPrimaryKey,
                cancellationToken);

            // 沒資料了 → 直接跳出 while，整個流程結束
            if (!rows.Any())
            {
                _logger.LogInformation(emptyMessageTemplate, setting.TableName);
                break;
            }

            // 2. 執行批次處理（搬移 / 匯出 + 刪除）
            var sw = Stopwatch.StartNew();
            await handleBatchAsync(setting, rows, cancellationToken);
            sw.Stop();

            // 3. 更新游標：使用這一批的「最後一筆」
            //    搭配 ORDER BY date, pk，下一輪會從這筆之後開始抓。
            var lastRow = rows[^1];

            if (!lastRow.TryGetValue(setting.DateColumn, out var dateValue) || dateValue is null)
            {
                throw new InvalidOperationException(
                    $"{setting.TableName} 搬移時，無法從資料列取得日期欄位 {setting.DateColumn}");
            }

            lastDate = Convert.ToDateTime(dateValue, CultureInfo.InvariantCulture);

            if (!lastRow.TryGetValue(setting.PrimaryKeyColumn, out var pkValue) || pkValue is null)
            {
                throw new InvalidOperationException(
                    $"{setting.TableName} 搬移時，無法從資料列取得主鍵欄位 {setting.PrimaryKeyColumn}");
            }

            // PK 不轉型，保持原本型別（int / guid / long...），後面 SQL 比較會用到
            lastPrimaryKey = pkValue;
            
            // 4. 根據這一批的耗時與筆數，動態調整下一次的 BatchSize
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
        // 若設定有指定 BatchSize 就用設定值，否則預設 1000
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
    /// 關鍵點：
    /// - WHERE DateColumn &lt; cutoff 控制「要搬移的時間範圍」
    /// - (LastDate, LastPrimaryKey) 控制「這一批從哪裡開始接著抓」，避免重複
    /// - ORDER BY Date, PK 保證游標推進的順序是穩定的
    /// </summary>
    private async Task<IReadOnlyList<IDictionary<string, object?>>> FetchBatchAsync(
        string connectionName,
        string tableName,
        string dateColumn,
        string primaryKeyColumn,
        DateTime cutoff,
        int batchSize,
        DateTime? lastDate,
        object? lastPrimaryKey,
        CancellationToken cancellationToken)
    {
        var sb = new StringBuilder();

        sb.AppendLine("SELECT TOP (@BatchSize) *");
        sb.AppendLine($"FROM [{tableName}] WITH (READPAST)"); // READPAST：跳過被鎖住的列，避免卡死
        sb.AppendLine($"WHERE [{dateColumn}] < @Cutoff");

        // 如果有上一批游標，則只抓「比上一筆晚」的資料
        // (Date > lastDate) OR (Date = lastDate AND PK > lastPk)
        // 搭配 ORDER BY Date, PK，可保證不重複也不漏資料
        if (lastDate.HasValue)
        {
            sb.AppendLine(
                $"  AND (([{dateColumn}] > @LastDate) " +
                $"OR ([{dateColumn}] = @LastDate AND [{primaryKeyColumn}] > @LastPrimaryKey))");
        }

        sb.AppendLine($"ORDER BY [{dateColumn}] ASC, [{primaryKeyColumn}] ASC;");

        var sql = sb.ToString();

        await using var connection = _connectionFactory.CreateConnection(connectionName);
        await connection.OpenAsync(cancellationToken);

        var parameters = new
        {
            BatchSize = batchSize,
            Cutoff = cutoff,
            // 若 lastDate 為 null，這兩個值實際上不會被用到（上面不會加游標條件）
            LastDate = (object?)lastDate ?? DBNull.Value,
            LastPrimaryKey = lastPrimaryKey ?? DBNull.Value
        };

        var command = new CommandDefinition(
            sql,
            parameters,
            cancellationToken: cancellationToken);

        // Dapper 回傳 dynamic，每列包成 IDictionary<string, object?>
        var rows = await connection.QueryAsync(command);
        return rows.Select(r => (IDictionary<string, object?>)r).ToList();
    }

    /// <summary>
    /// 單批：線上庫 → 歷史庫，BulkInsert，並視設定決定是否刪除來源資料。
    /// - 這個方法只處理「一批」資料，呼叫方會負責 while + 游標。
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
        // 提前收集 PK，是為了之後 DeleteByPrimaryKeys 使用
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

        // 1. 寫到歷史庫，拿到實際匯入的筆數（已排除歷史本來就有的 PK）
        var insertedCount = await BulkInsertAsync(setting, rows, cancellationToken);
        var sourceCount = rows.Count;
        var skippedCount = sourceCount - insertedCount;

        if (setting.IsPhysicalDeleteEnabled)
        {
            // 2. 刪掉來源庫的這批資料（不管有沒有成功插入，都刪）
            await DeleteByPrimaryKeysAsync(
                setting.SourceConnectionName,
                setting.TableName,
                setting.PrimaryKeyColumn,
                primaryKeys,
                archiveCommandTimeout,
                cancellationToken);

            _logger.LogInformation(
                "{Table} 搬移批次完成：來源 {Source} 筆，新增 {Inserted} 筆到歷史，刪除來源 {Deleted} 筆（其中 {Skipped} 筆歷史已存在）。",
                setting.TableName,
                sourceCount,
                insertedCount,
                primaryKeys.Count,
                skippedCount);
        }
        else
        {
            // 不刪來源：線上資料會繼續存在，歷史只會補上缺的那幾筆
            _logger.LogInformation(
                "{Table} 搬移批次完成：來源 {Source} 筆，新增 {Inserted} 筆到歷史，保留來源資料（{Skipped} 筆歷史已存在）。",
                setting.TableName,
                sourceCount,
                insertedCount,
                skippedCount);
        }
    }

    /// <summary>
    /// 歷史庫依主鍵批次刪除資料，內部也會拆批避免 2100 參數上限。
    /// 用在「歷史 → CSV」流程中，刪掉已匯出的歷史資料。
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

        // DynamicSqlHelper 內部會產生類似：
        // DELETE FROM [Table] WHERE [PK] IN @Ids;
        var deleteSql = DynamicSqlHelper.BuildDeleteSql(tableName, primaryKeyColumn);

        await using var connection = _connectionFactory.CreateConnection(connectionName);
        await connection.OpenAsync(cancellationToken);

        // 這裡是第二層的「拆批」，避免單次 IN 參數超過 2100 上限
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
    /// - 會依 MaxRowsPerFile 切成多個檔
    /// - 檔名依 Table + 日期範圍 + PartIndex 組出來
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
        // 依 config.MaxRowsPerFile 把一大批再切成多批寫入不同檔案
        var chunks = ChunkRows(rows, _csvOptions.MaxRowsPerFile).ToList();

        // 確保資料夾存在（多呼叫沒差）
        Directory.CreateDirectory(ResolveCsvDirectory(setting, toDate));

        for (var i = 0; i < chunks.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var filePath = ResolveCsvPath(setting, fromDate, toDate, i + 1);
            // BOM = true，避免 Excel 亂碼
            await using var writer = new StreamWriter(filePath, append: false, encoding: new UTF8Encoding(true));

            // 寫欄位名稱列
            await writer.WriteLineAsync(string.Join(_csvOptions.Delimiter, columns));

            // 寫每一列資料
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
    /// 用 DateColumn 的 Min/Max 當作 From/To。
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

    /// <summary>
    /// 把一個大 List 切成多個固定大小的小 List。
    /// </summary>
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
    /// 組出 CSV 存放目錄：根目錄 / TableName / yyyyMM
    /// </summary>
    private string ResolveCsvDirectory(ArchiveSetting setting, DateTime targetDate)
    {
        var folder = Path.Combine(
            setting.CsvRootFolder,
            setting.TableName,
            targetDate.ToString("yyyyMM"));

        Directory.CreateDirectory(folder);
        return folder;
    }

    /// <summary>
    /// 組出 CSV 檔案完整路徑（含檔名）。
    /// FileNameFormat 由設定決定，這裡只做 placeholder 替換。
    /// </summary>
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

    /// <summary>
    /// CSV 欄位內容 Escape：
    /// - 若包含 delimiter / 雙引號 / 換行，就整個加雙引號並把內部 " 變成 ""。
    /// </summary>
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
    /// - 太慢：減少 BatchSize（對半砍，但不低於 Min）
    /// - 很快且有跑滿：加大 BatchSize（加倍，但不超過 Max）
    /// - 其他情況：維持原樣
    /// 目標是盡量讓每批執行時間落在 TargetBatchSeconds 附近。
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
            // 沒資料其實也不會再用到下一批，但維持原值即可
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
    /// 回傳實際寫入歷史表的筆數。
    /// </summary>
    private async Task<int> BulkInsertAsync(
        ArchiveSetting setting,
        IReadOnlyList<IDictionary<string, object?>> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return 0;
        }

        var columns = rows[0].Keys.ToList();

        if (!columns.Contains(setting.PrimaryKeyColumn))
        {
            throw new InvalidOperationException(
                $"BulkInsert {setting.TableName} 時找不到主鍵欄位 {setting.PrimaryKeyColumn}");
        }

        // 1. 先整理這批要搬的主鍵清單（全部轉成字串）
        //    之後查歷史庫現有 PK 時都用字串比對，避免型別差異。
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

        // 這裡再用 MaxSqlParametersPerCommand 做一次拆批，避免 IN 參數過多
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
                    // 主鍵為 null 的列一律略過
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
            return 0;
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
            // 這裡用 object，讓各種型別都能塞進來，交給 SqlBulkCopy 自己處理型別
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

        // 這裡假設來源欄位名稱 = 目的欄位名稱，一一對應
        foreach (var column in columns)
        {
            bulkCopy.ColumnMappings.Add(column, column);
        }

        await bulkCopy.WriteToServerAsync(table, cancellationToken);

        // 回傳實際寫入歷史表的筆數
        return filteredRows.Count;
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
/// Succeeded：整次 Run 是否成功
/// Messages：每個 Table 的處理訊息 / 錯誤說明
/// </summary>
public record MigrationResult(bool Succeeded, IReadOnlyList<string> Messages);
