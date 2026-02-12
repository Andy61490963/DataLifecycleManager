using System.Collections.Generic;
using System.Linq;
using Dapper;
using DataLifecycleManager.Infrastructure;
using DataLifecycleManager.Models;
using DataLifecycleManager.ViewModels;

namespace DataLifecycleManager.Repositories;

/// <summary>
/// 定義讀寫 ArchiveJobRun 與 ArchiveJobRunDetail 紀錄的介面，透過 Dapper 操作資料庫。
/// </summary>
public interface IArchiveJobLogService
{
    /// <summary>新增整體搬移工作的執行紀錄。</summary>
    Task CreateJobRunAsync(ArchiveJobRunLog jobRun, CancellationToken cancellationToken);

    /// <summary>更新整體搬移工作的執行紀錄。</summary>
    Task UpdateJobRunAsync(ArchiveJobRunLog jobRun, CancellationToken cancellationToken);

    /// <summary>新增單一資料表搬移的執行紀錄。</summary>
    Task CreateDetailAsync(ArchiveJobRunDetailLog detail, CancellationToken cancellationToken);

    /// <summary>更新單一資料表搬移的執行紀錄。</summary>
    Task UpdateDetailAsync(ArchiveJobRunDetailLog detail, CancellationToken cancellationToken);

    /// <summary>取得所有搬移工作及其詳細紀錄。</summary>
    Task<IEnumerable<ArchiveJobRunLogWithDetails>> GetJobRunsWithDetailsAsync(CancellationToken cancellationToken);
}

/// <summary>
/// 使用 Dapper 操作 ArchiveJobRun / ArchiveJobRunDetail 兩張 Log Table。
/// </summary>
public class ArchiveJobLogService : IArchiveJobLogService
{
    private readonly SqlConnectionFactory _connectionFactory;

    // 這裡假設 log 跟設定放同一顆 DB，用同一個 connectionName。
    // 如果你有獨立的管理 DB，可以另外丟一個 options 進來。
    private const string ConfigurationConnectionName = "ConfigurationDb";

    /// <summary>
    /// 以連線工廠建立服務實例，確保所有查詢皆共用一致的資料來源設定。
    /// </summary>
    public ArchiveJobLogService(SqlConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    /// <summary>
    /// 新增整體搬移工作的執行紀錄，記錄開始時間與初始狀態。
    /// </summary>
    public async Task CreateJobRunAsync(ArchiveJobRunLog jobRun, CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO ArchiveJobRun (
    JobRunId,
    StartedAt,
    EndedAt,
    Status,
    HostName,
    TotalTables,
    SucceededTables,
    FailedTables,
    Message
)
VALUES (
    @JobRunId,
    @StartedAt,
    @EndedAt,
    @Status,
    @HostName,
    @TotalTables,
    @SucceededTables,
    @FailedTables,
    @Message
);";

        await using var conn = _connectionFactory.CreateConnection(ConfigurationConnectionName);
        await conn.OpenAsync(cancellationToken);

        var cmd = new CommandDefinition(sql, jobRun, cancellationToken: cancellationToken);
        await conn.ExecuteAsync(cmd);
    }

    /// <summary>
    /// 更新整體搬移工作的執行紀錄，通常於流程結束時寫入統計數據與狀態。
    /// </summary>
    public async Task UpdateJobRunAsync(ArchiveJobRunLog jobRun, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE ArchiveJobRun
SET
    StartedAt       = @StartedAt,
    EndedAt         = @EndedAt,
    Status          = @Status,
    HostName        = @HostName,
    TotalTables     = @TotalTables,
    SucceededTables = @SucceededTables,
    FailedTables    = @FailedTables,
    Message         = @Message
WHERE JobRunId = @JobRunId;";

        await using var conn = _connectionFactory.CreateConnection(ConfigurationConnectionName);
        await conn.OpenAsync(cancellationToken);

        var cmd = new CommandDefinition(sql, jobRun, cancellationToken: cancellationToken);
        await conn.ExecuteAsync(cmd);
    }

    /// <summary>
    /// 新增單一資料表搬移的執行紀錄，保存當下設定快照與開始時間。
    /// </summary>
    public async Task CreateDetailAsync(ArchiveJobRunDetailLog detail, CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO ArchiveJobRunDetail (
    TableRunId,
    JobRunId,
    SettingId,
    SourceConnectionName,
    TargetConnectionName,
    TableName,
    DateColumn,
    PrimaryKeyColumn,
    OnlineRetentionDate,
    HistoryRetentionDate,
    BatchSize,
    CsvEnabled,
    CsvRootFolder,
    IsPhysicalDeleteEnabled,
    StartedAt,
    EndedAt,
    Status,
    TotalSourceScanned,
    TotalInsertedToHistory,
    TotalDeletedFromSource,
    TotalExportedToCsv,
    TotalDeletedFromHistory,
    LastProcessedDate,
    LastProcessedPrimaryKey,
    ErrorMessage
)
VALUES (
    @TableRunId,
    @JobRunId,
    @SettingId,
    @SourceConnectionName,
    @TargetConnectionName,
    @TableName,
    @DateColumn,
    @PrimaryKeyColumn,
    @OnlineRetentionDate,
    @HistoryRetentionDate,
    @BatchSize,
    @CsvEnabled,
    @CsvRootFolder,
    @IsPhysicalDeleteEnabled,
    @StartedAt,
    @EndedAt,
    @Status,
    @TotalSourceScanned,
    @TotalInsertedToHistory,
    @TotalDeletedFromSource,
    @TotalExportedToCsv,
    @TotalDeletedFromHistory,
    @LastProcessedDate,
    @LastProcessedPrimaryKey,
    @ErrorMessage
);";

        await using var conn = _connectionFactory.CreateConnection(ConfigurationConnectionName);
        await conn.OpenAsync(cancellationToken);

        var cmd = new CommandDefinition(sql, detail, cancellationToken: cancellationToken);
        await conn.ExecuteAsync(cmd);
    }

    /// <summary>
    /// 更新單一資料表搬移的執行紀錄，寫入結束時間、狀態與統計數據。
    /// </summary>
    public async Task UpdateDetailAsync(ArchiveJobRunDetailLog detail, CancellationToken cancellationToken)
    {
        const string sql = @"
UPDATE ArchiveJobRunDetail
SET
    JobRunId                = @JobRunId,
    SettingId               = @SettingId,
    SourceConnectionName    = @SourceConnectionName,
    TargetConnectionName    = @TargetConnectionName,
    TableName               = @TableName,
    DateColumn              = @DateColumn,
    PrimaryKeyColumn        = @PrimaryKeyColumn,
    OnlineRetentionDate     = @OnlineRetentionDate,
    HistoryRetentionDate    = @HistoryRetentionDate,
    BatchSize               = @BatchSize,
    CsvEnabled              = @CsvEnabled,
    CsvRootFolder           = @CsvRootFolder,
    IsPhysicalDeleteEnabled = @IsPhysicalDeleteEnabled,
    StartedAt               = @StartedAt,
    EndedAt                 = @EndedAt,
    Status                  = @Status,
    TotalSourceScanned      = @TotalSourceScanned,
    TotalInsertedToHistory  = @TotalInsertedToHistory,
    TotalDeletedFromSource  = @TotalDeletedFromSource,
    TotalExportedToCsv      = @TotalExportedToCsv,
    TotalDeletedFromHistory = @TotalDeletedFromHistory,
    LastProcessedDate       = @LastProcessedDate,
    LastProcessedPrimaryKey = @LastProcessedPrimaryKey,
    ErrorMessage            = @ErrorMessage
WHERE TableRunId = @TableRunId;";

        await using var conn = _connectionFactory.CreateConnection(ConfigurationConnectionName);
        await conn.OpenAsync(cancellationToken);

        var cmd = new CommandDefinition(sql, detail, cancellationToken: cancellationToken);
        await conn.ExecuteAsync(cmd);
    }

    /// <summary>
    /// 取得所有搬移工作與其對應的資料表紀錄，供後台頁面一次性渲染。
    /// </summary>
    public async Task<IEnumerable<ArchiveJobRunLogWithDetails>> GetJobRunsWithDetailsAsync(CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT JobRunId, StartedAt, EndedAt, Status, HostName, TotalTables, SucceededTables, FailedTables, Message
FROM ArchiveJobRun
ORDER BY StartedAt DESC;

SELECT TableRunId, JobRunId, SettingId, SourceConnectionName, TargetConnectionName, TableName, DateColumn, PrimaryKeyColumn,
       OnlineRetentionDate, HistoryRetentionDate, BatchSize, CsvEnabled, CsvRootFolder, IsPhysicalDeleteEnabled, StartedAt,
       EndedAt, Status, TotalSourceScanned, TotalInsertedToHistory, TotalDeletedFromSource, TotalExportedToCsv,
       TotalDeletedFromHistory, LastProcessedDate, LastProcessedPrimaryKey, ErrorMessage
FROM ArchiveJobRunDetail
ORDER BY StartedAt DESC;";

        await using var conn = _connectionFactory.CreateConnection(ConfigurationConnectionName);
        await conn.OpenAsync(cancellationToken);

        var cmd = new CommandDefinition(sql, cancellationToken: cancellationToken);
        using var multi = await conn.QueryMultipleAsync(cmd);

        var jobs = (await multi.ReadAsync<ArchiveJobRunLog>()).ToList();
        var details = (await multi.ReadAsync<ArchiveJobRunDetailLog>()).ToList();

        var detailLookup = details.GroupBy(d => d.JobRunId)
            .ToDictionary(g => g.Key, g => (IEnumerable<ArchiveJobRunDetailLog>)g.ToList());

        return jobs.Select(job => new ArchiveJobRunLogWithDetails
        {
            JobRun = job,
            Details = detailLookup.TryGetValue(job.JobRunId, out var detailList)
                ? detailList
                : Enumerable.Empty<ArchiveJobRunDetailLog>()
        });
    }
}
