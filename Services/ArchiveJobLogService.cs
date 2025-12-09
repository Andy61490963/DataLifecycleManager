using System.Text;
using Dapper;
using DataLifecycleManager.Infrastructure;
using DataLifecycleManager.Models;

namespace DataLifecycleManager.Repositories;

/// <summary>
/// 使用 Dapper 操作 ArchiveJobRun / ArchiveJobRunDetail 兩張 Log Table。
/// </summary>
public class ArchiveJobLogService
{
    private readonly SqlConnectionFactory _connectionFactory;

    // 這裡假設 log 跟設定放同一顆 DB，用同一個 connectionName。
    // 如果你有獨立的管理 DB，可以另外丟一個 options 進來。
    private const string ConfigurationConnectionName = "ConfigurationDb";

    public ArchiveJobLogService(SqlConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

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
}
