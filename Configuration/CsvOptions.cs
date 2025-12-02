using System.ComponentModel.DataAnnotations;

namespace DataLifecycleManager.Configuration;

/// <summary>
/// CSV 匯出相關固定參數設定。
/// </summary>
public class CsvOptions
{
    /// <summary>欄位分隔符號。</summary>
    public string Delimiter { get; set; } = ",";

    /// <summary>單一 CSV 最大行數。</summary>
    [Range(1, int.MaxValue)]
    public int MaxRowsPerFile { get; set; } = 100_000;

    /// <summary>檔名格式。</summary>
    public string FileNameFormat { get; set; } = "{TableName}_{FromDate:yyyyMMdd}_{ToDate:yyyyMMdd}_Part{PartIndex}.csv";
}
