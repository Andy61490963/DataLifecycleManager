using System.ComponentModel.DataAnnotations;

namespace DataLifecycleManager.ViewModels;

/// <summary>
/// 設定頁面使用的表單輸入模型，包含資料驗證屬性。
/// </summary>
public class ArchiveSettingInputModel
{
    /// <summary>設定編號（新增時可為 null）。</summary>
    public int? Id { get; set; }

    /// <summary>來源資料庫的連線名稱。</summary>
    [Required]
    [Display(Name = "來源資料庫的連線字串")]
    public string SourceConnectionName { get; set; } = string.Empty;

    /// <summary>目標資料庫的連線名稱。</summary>
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

    /// <summary>線上保留月份。</summary>
    [Range(1, 120)]
    [Display(Name = "線上保留月份")]
    public int OnlineRetentionMonths { get; set; } = 3;

    /// <summary>歷史保留月份。</summary>
    [Range(1, 120)]
    [Display(Name = "歷史保留月份")]
    public int HistoryRetentionMonths { get; set; } = 6;

    /// <summary>每批處理筆數。</summary>
    [Range(1, int.MaxValue)]
    [Display(Name = "每批處理筆數")]
    public int BatchSize { get; set; } = 2000;

    /// <summary>是否啟用 CSV 匯出。</summary>
    [Display(Name = "是否啟用 CSV 匯出")]
    public bool CsvEnabled { get; set; } = true;

    /// <summary>CSV 根目錄。</summary>
    [Required]
    [Display(Name = "CSV 根目錄")]
    public string CsvRootFolder { get; set; } = string.Empty;
}
