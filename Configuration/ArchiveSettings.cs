using System.ComponentModel.DataAnnotations;

namespace DataLifecycleManager.Configuration;

/// <summary>
/// 歸檔與匯出相關的設定總表，提供預設值與各表覆蓋選項。
/// </summary>
public class ArchiveSettings
{
    /// <summary>是否啟用整體歸檔機制。</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>預設單批處理筆數。</summary>
    [Range(1, int.MaxValue)]
    public int DefaultBatchSize { get; set; } = 5_000;

    /// <summary>預設線上庫保留月數。</summary>
    [Range(1, 120)]
    public int DefaultOnlineRetentionMonths { get; set; } = 3;

    /// <summary>預設歷史庫保留月數，超過後匯出 CSV。</summary>
    [Range(1, 120)]
    public int DefaultHistoryRetentionMonths { get; set; } = 6;

    /// <summary>CSV 匯出相關設定。</summary>
    public CsvSettings Csv { get; set; } = new();

    /// <summary>簡易重試策略設定。</summary>
    public RetryPolicySettings RetryPolicy { get; set; } = new();

    /// <summary>各表個別設定覆蓋。</summary>
    public List<TableSettings> Tables { get; set; } = new();
}

/// <summary>CSV 匯出設定值。</summary>
public class CsvSettings
{
    /// <summary>是否啟用 CSV 匯出。</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>CSV 存放根目錄。</summary>
    public string RootFolder { get; set; } = "D:/ArchiveCsv";

    /// <summary>單一 CSV 最大行數。</summary>
    [Range(1, int.MaxValue)]
    public int MaxRowsPerFile { get; set; } = 100_000;

    /// <summary>欄位分隔符號。</summary>
    public string Delimiter { get; set; } = ",";

    /// <summary>檔名格式。</summary>
    public string FileNameFormat { get; set; } = "{TableName}_{FromDate:yyyyMMdd}_{ToDate:yyyyMMdd}_Part{PartIndex}.csv";
}

/// <summary>重試策略設定值。</summary>
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

/// <summary>各表的個別設定。</summary>
public class TableSettings
{
    /// <summary>表名。</summary>
    [Required]
    public string Name { get; set; } = string.Empty;

    /// <summary>來源連線設定名稱。</summary>
    [Required]
    public string SourceConnectionName { get; set; } = "Db1";

    /// <summary>目標連線設定名稱。</summary>
    [Required]
    public string TargetConnectionName { get; set; } = "Db2";

    /// <summary>判斷時間的欄位名稱。</summary>
    [Required]
    public string DateColumn { get; set; } = string.Empty;

    /// <summary>主鍵欄位名稱。</summary>
    [Required]
    public string PrimaryKeyColumn { get; set; } = string.Empty;

    /// <summary>線上庫保留月數，未設定則使用預設值。</summary>
    public int? OnlineRetentionMonths { get; set; }

    /// <summary>歷史庫保留月數，未設定則使用預設值。</summary>
    public int? HistoryRetentionMonths { get; set; }

    /// <summary>批次處理筆數，未設定則使用預設值。</summary>
    public int? BatchSize { get; set; }

    /// <summary>是否啟用 CSV 匯出。</summary>
    public bool CsvEnabled { get; set; } = true;
}
