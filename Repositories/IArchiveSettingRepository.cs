using DataLifecycleManager.Models;

namespace DataLifecycleManager.Repositories;

/// <summary>
/// 存取搬移設定資料表的 Repository 介面。
/// </summary>
public interface IArchiveSettingRepository
{
    /// <summary>
    /// 讀取所有設定紀錄，供 UI 與搬移流程使用。
    /// </summary>
    /// <param name="cancellationToken">取消權杖。</param>
    /// <returns>設定集合。</returns>
    Task<IReadOnlyList<ArchiveSetting>> GetAllAsync(CancellationToken cancellationToken);

    /// <summary>
    /// 依主鍵查詢單一設定紀錄。
    /// </summary>
    /// <param name="id">設定主鍵。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    /// <returns>找到的設定；若無則為 null。</returns>
    Task<ArchiveSetting?> GetByIdAsync(int id, CancellationToken cancellationToken);

    /// <summary>
    /// 新增或更新單筆設定紀錄。
    /// </summary>
    /// <param name="setting">設定內容。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    /// <returns>資料庫中的主鍵值。</returns>
    Task<int> UpsertAsync(ArchiveSetting setting, CancellationToken cancellationToken);

    /// <summary>
    /// 刪除指定的設定紀錄。
    /// </summary>
    /// <param name="id">設定主鍵。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    Task DeleteAsync(int id, CancellationToken cancellationToken);
}
