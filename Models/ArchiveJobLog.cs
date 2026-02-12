namespace DataLifecycleManager.Models;

/// <summary>
/// Job 整體執行狀態文字常數，對應 DB ArchiveJobRun.Status / ArchiveJobRunDetail.Status
/// </summary>
public static class ArchiveJobStatus
{
    public const string Running     = "Running";
    public const string Success     = "Success";
    public const string PartialFail = "PartialFail";
    public const string Fail        = "Fail";
    public const string Skipped     = "Skipped";
}

/// <summary>
/// 對應 ArchiveJobRun Table
/// </summary>
public sealed class ArchiveJobRunLog
{
    public Guid JobRunId { get; set; }
    public DateTime StartedAt { get; set; }
    public DateTime? EndedAt { get; set; }
    public string Status { get; set; } = ArchiveJobStatus.Running;
    public string? HostName { get; set; }
    public int TotalTables { get; set; }
    public int SucceededTables { get; set; }
    public int FailedTables { get; set; }
    public string? Message { get; set; }
}

/// <summary>
/// 對應 ArchiveJobRunDetail Table
/// </summary>
public sealed class ArchiveJobRunDetailLog
{
    public Guid TableRunId { get; set; }
    public Guid JobRunId { get; set; }
    public int SettingId { get; set; }                // 對應 ArchiveSettings PK

    public string SourceConnectionName { get; set; } = default!;
    public string TargetConnectionName { get; set; } = default!;
    public string TableName { get; set; } = default!;
    public string DateColumn { get; set; } = default!;
    public string PrimaryKeyColumn { get; set; } = default!;

    public DateTime OnlineRetentionDate { get; set; }
    public DateTime HistoryRetentionDate { get; set; }
    public int BatchSize { get; set; }

    public bool CsvEnabled { get; set; }
    public string CsvRootFolder { get; set; } = default!;
    public bool IsPhysicalDeleteEnabled { get; set; }

    public DateTime StartedAt { get; set; }
    public DateTime? EndedAt { get; set; }
    public string Status { get; set; } = ArchiveJobStatus.Running;

    public int TotalSourceScanned { get; set; }
    public int TotalInsertedToHistory { get; set; }
    public int? TotalDeletedFromSource { get; set; }
    public int TotalExportedToCsv { get; set; }
    public int? TotalDeletedFromHistory { get; set; }

    public DateTime? LastProcessedDate { get; set; }
    public string? LastProcessedPrimaryKey { get; set; }

    public string? ErrorMessage { get; set; }
}