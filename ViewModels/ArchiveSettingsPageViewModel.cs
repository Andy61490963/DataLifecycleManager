using DataLifecycleManager.Models;

namespace DataLifecycleManager.ViewModels;

/// <summary>
/// 設定頁面整體 ViewModel，提供表單輸入、既有設定列表以及執行結果。
/// </summary>
public class ArchiveSettingsPageViewModel
{
    /// <summary>目前編輯中的設定表單。</summary>
    public ArchiveSettingInputModel Form { get; set; } = new();

    /// <summary>資料庫中已存在的設定列表。</summary>
    public IReadOnlyList<ArchiveSetting> ExistingSettings { get; set; } = Array.Empty<ArchiveSetting>();

    /// <summary>最新一次搬移的訊息列表。</summary>
    public IReadOnlyList<string> ExecutionMessages { get; set; } = Array.Empty<string>();

    /// <summary>最新一次搬移的狀態。</summary>
    public bool? ExecutionSucceeded { get; set; }
}
