using System.ComponentModel.DataAnnotations;

namespace DataLifecycleManager.ViewModels;

/// <summary>
/// 設定頁面使用的表單輸入模型，包含資料驗證屬性。
/// </summary>
public class ArchiveSettingInputModel : IValidatableObject
{
    /// <summary>設定編號（新增時可為 null）。</summary>
    public int? Id { get; set; }

    /// <summary>來源資料庫的連線名稱或完整連線字串。</summary>
    [Required]
    [Display(Name = "來源資料庫的連線字串")]
    public string SourceConnectionName { get; set; } = string.Empty;

    /// <summary>目標資料庫的連線名稱或完整連線字串。</summary>
    [Required]
    [Display(Name = "目標資料庫的連線字串")]
    public string TargetConnectionName { get; set; } = string.Empty;

    /// <summary>搬移的資料表名稱。</summary>
    [Required]
    [Display(Name = "搬移的資料表名稱")]
    public string TableName { get; set; } = string.Empty;

    /// <summary>日期判斷欄位名稱。</summary>
    [Required]
    [Display(Name = "日期判斷欄位名稱")]
    public string DateColumn { get; set; } = string.Empty;

    /// <summary>主鍵欄位名稱。</summary>
    [Required]
    [Display(Name = "主鍵欄位名稱")]
    public string PrimaryKeyColumn { get; set; } = string.Empty;

    /// <summary>線上資料保留截止日期。</summary>
    [DataType(DataType.Date)]
    [Display(Name = "線上資料保留截止日期")]
    public DateTime OnlineRetentionDate { get; set; } = DateTime.Today.AddMonths(-3);

    /// <summary>歷史資料保留截止日期。</summary>
    [DataType(DataType.Date)]
    [Display(Name = "歷史資料保留截止日期")]
    public DateTime HistoryRetentionDate { get; set; } = DateTime.Today.AddMonths(-6);

    /// <summary>每批處理筆數。</summary>
    [Range(1, int.MaxValue)]
    [Display(Name = "每批處理筆數(不得超過4000)")]
    public int BatchSize { get; set; } = 2000;

    /// <summary>是否啟用 CSV 匯出。</summary>
    [Display(Name = "是否啟用 CSV 匯出")]
    public bool CsvEnabled { get; set; } = true;

    /// <summary>CSV 根目錄。</summary>
    [Required]
    [Display(Name = "CSV 根目錄")]
    public string CsvRootFolder { get; set; } = string.Empty;

    /// <summary>是否啟用此搬移設定。</summary>
    [Display(Name = "是否啟用此設定")]
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// 驗證表單邏輯：線上保留日期必須晚於歷史保留日期，避免搬移流程產生矛盾區間。
    /// </summary>
    /// <param name="validationContext">驗證內容。</param>
    /// <returns>驗證錯誤集合。</returns>
    public IEnumerable<ValidationResult> Validate(ValidationContext validationContext)
    {
        if (OnlineRetentionDate <= HistoryRetentionDate)
        {
            yield return new ValidationResult(
                "線上資料保留截止日期必須晚於歷史資料保留截止日期。",
                new[] { nameof(OnlineRetentionDate), nameof(HistoryRetentionDate) });
        }
    }
}
