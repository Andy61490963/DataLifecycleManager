using System.Collections.Generic;
using System.Linq;
using DataLifecycleManager.Models;

namespace DataLifecycleManager.ViewModels;

/// <summary>
/// 彙總單次搬移工作與對應的資料表明細，方便於畫面一次呈現。
/// </summary>
public sealed class ArchiveJobRunLogWithDetails
{
    /// <summary>搬移工作的主檔紀錄。</summary>
    public ArchiveJobRunLog JobRun { get; init; } = default!;

    /// <summary>搬移工作底下的所有資料表執行紀錄。</summary>
    public IEnumerable<ArchiveJobRunDetailLog> Details { get; init; } = Enumerable.Empty<ArchiveJobRunDetailLog>();
}
