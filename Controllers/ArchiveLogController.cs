using DataLifecycleManager.Repositories;
using DataLifecycleManager.ViewModels;
using Microsoft.AspNetCore.Mvc;

namespace DataLifecycleManager.Controllers;

/// <summary>
/// 提供搬移紀錄檢視的 MVC 控制器，供使用者查詢歷史執行結果。
/// </summary>
public class ArchiveLogController : Controller
{
    private readonly IArchiveJobLogService _archiveJobLogService;

    /// <summary>
    /// 透過日誌服務取得搬移紀錄並渲染對應頁面。
    /// </summary>
    /// <param name="archiveJobLogService">Dapper 驅動的搬移日誌查詢服務。</param>
    public ArchiveLogController(IArchiveJobLogService archiveJobLogService)
    {
        _archiveJobLogService = archiveJobLogService;
    }

    /// <summary>
    /// 顯示所有搬移工作與資料表的詳細紀錄，方便追蹤每次執行狀態。
    /// </summary>
    /// <param name="ct">取消權杖。</param>
    /// <returns>搬移紀錄檢視頁。</returns>
    [HttpGet]
    public async Task<IActionResult> Index(CancellationToken ct)
    {
        var logs = await _archiveJobLogService.GetJobRunsWithDetailsAsync(ct);
        return View(logs);
    }
}
