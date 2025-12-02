namespace DataLifecycleManager.Domain;

/// <summary>
/// 提供從資料庫或其他來源載入歸檔任務設定的介面。
/// </summary>
public interface IArchiveSettingsProvider
{
    /// <summary>
    /// 載入所有啟用的歸檔任務設定並套用預設值。
    /// </summary>
    /// <param name="cancellationToken">取消權杖。</param>
    /// <returns>可執行的任務設定集合。</returns>
    Task<IReadOnlyList<ArchiveJobRuntimeSettings>> GetJobsAsync(CancellationToken cancellationToken);
}
