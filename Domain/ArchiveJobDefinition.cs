using System.ComponentModel.DataAnnotations;

namespace DataLifecycleManager.Domain;

/// <summary>
/// 從資料庫取得的歸檔任務定義。
/// </summary>
public class ArchiveJobDefinition
{
    /// <summary>流水號。</summary>
    public int Id { get; set; }

    /// <summary>任務名稱。</summary>
    [Required, StringLength(128)]
    public string JobName { get; set; } = string.Empty;

    /// <summary>來源資料表名稱。</summary>
    [Required, StringLength(256)]
    public string TableName { get; set; } = string.Empty;

    /// <summary>來源連線名稱（對應 ConnectionStrings）。</summary>
    [Required, StringLength(128)]
    public string SourceConnection { get; set; } = string.Empty;

    /// <summary>目標連線名稱。</summary>
    [Required, StringLength(128)]
    public string TargetConnection { get; set; } = string.Empty;

    /// <summary>判斷日期欄位。</summary>
    [Required, StringLength(128)]
    public string DateColumn { get; set; } = string.Empty;

    /// <summary>主鍵欄位。</summary>
    [Required, StringLength(128)]
    public string PrimaryKeyColumn { get; set; } = string.Empty;

    /// <summary>線上庫保留月數。</summary>
    public int OnlineRetentionMonths { get; set; }

    /// <summary>歷史庫保留月數。</summary>
    public int HistoryRetentionMonths { get; set; }

    /// <summary>批次大小。</summary>
    public int BatchSize { get; set; }

    /// <summary>是否啟用 CSV。</summary>
    public bool CsvEnabled { get; set; }

    /// <summary>個別 CSV 根目錄。</summary>
    public string? CsvRootFolder { get; set; }

    /// <summary>是否啟用任務。</summary>
    public bool Enabled { get; set; }

    /// <summary>重試策略設定（若無則使用預設）。</summary>
    public RetryPolicySettings? RetryPolicy { get; set; }
}

/// <summary>
/// 合併預設值後供執行使用的設定。
/// </summary>
public class ArchiveJobRuntimeSettings
{
    /// <summary>任務 Id。</summary>
    public required int Id { get; init; }

    /// <summary>任務名稱。</summary>
    public required string JobName { get; init; }

    /// <summary>資料表名稱。</summary>
    public required string TableName { get; init; }

    /// <summary>來源連線名稱。</summary>
    public required string SourceConnection { get; init; }

    /// <summary>目標連線名稱。</summary>
    public required string TargetConnection { get; init; }

    /// <summary>日期欄位名稱。</summary>
    public required string DateColumn { get; init; }

    /// <summary>主鍵欄位名稱。</summary>
    public required string PrimaryKeyColumn { get; init; }

    /// <summary>線上保留月數。</summary>
    public required int OnlineRetentionMonths { get; init; }

    /// <summary>歷史保留月數。</summary>
    public required int HistoryRetentionMonths { get; init; }

    /// <summary>批次大小。</summary>
    public required int BatchSize { get; init; }

    /// <summary>是否啟用 CSV。</summary>
    public required bool CsvEnabled { get; init; }

    /// <summary>CSV 根目錄。</summary>
    public required string CsvRootFolder { get; init; }

    /// <summary>重試策略。</summary>
    public required RetryPolicySettings RetryPolicy { get; init; }
}

/// <summary>
/// 重試策略設定。
/// </summary>
public class RetryPolicySettings
{
    /// <summary>是否啟用重試。</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>最大重試次數。</summary>
    [Range(0, 10)]
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>重試間隔秒數。</summary>
    [Range(0, 300)]
    public int RetryDelaySeconds { get; set; } = 10;
}
