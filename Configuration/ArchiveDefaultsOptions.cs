using System.ComponentModel.DataAnnotations;
using DataLifecycleManager.Domain;

namespace DataLifecycleManager.Configuration;

/// <summary>
/// 歸檔作業的全域預設值設定，作為 DB 設定的安全 fallback。
/// </summary>
public class ArchiveDefaultsOptions
{
    /// <summary>是否啟用整體歸檔機制。</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>預設批次大小。</summary>
    [Range(1, int.MaxValue)]
    public int DefaultBatchSize { get; set; } = 5_000;

    /// <summary>線上庫保留月數預設值。</summary>
    [Range(1, 120)]
    public int DefaultOnlineRetentionMonths { get; set; } = 3;

    /// <summary>歷史庫保留月數預設值。</summary>
    [Range(1, 120)]
    public int DefaultHistoryRetentionMonths { get; set; } = 6;

    /// <summary>CSV 匯出相關預設值。</summary>
    public CsvDefaults Csv { get; set; } = new();

    /// <summary>重試策略預設值。</summary>
    public RetryPolicySettings RetryPolicy { get; set; } = new();
}

/// <summary>CSV 預設設定。</summary>
public class CsvDefaults
{
    /// <summary>預設是否啟用 CSV。</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>全域根目錄。</summary>
    [Required]
    public string RootFolder { get; set; } = "C:/ArchiveCsv";

    /// <summary>單檔最大行數。</summary>
    [Range(1, int.MaxValue)]
    public int MaxRowsPerFile { get; set; } = 100_000;

    /// <summary>欄位分隔符號。</summary>
    [Required]
    public string Delimiter { get; set; } = ",";

    /// <summary>檔名格式。</summary>
    [Required]
    public string FileNameFormat { get; set; } = "{TableName}_{FromDate:yyyyMMdd}_{ToDate:yyyyMMdd}_Part{PartIndex}.csv";
}
