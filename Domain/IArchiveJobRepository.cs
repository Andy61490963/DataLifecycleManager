namespace DataLifecycleManager.Domain;

/// <summary>
/// 封裝歸檔任務設定的資料存取。
/// </summary>
public interface IArchiveJobRepository
{
    /// <summary>
    /// 取得所有啟用的歸檔任務設定（含重試策略）。
    /// </summary>
    /// <param name="cancellationToken">取消權杖。</param>
    /// <returns>啟用的任務集合。</returns>
    Task<IReadOnlyList<ArchiveJobDefinition>> GetEnabledJobsAsync(CancellationToken cancellationToken);
}
