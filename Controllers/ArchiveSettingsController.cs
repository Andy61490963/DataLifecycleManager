using DataLifecycleManager.Models;
using DataLifecycleManager.Repositories;
using DataLifecycleManager.Services;
using DataLifecycleManager.ViewModels;
using Microsoft.AspNetCore.Mvc;

namespace DataLifecycleManager.Controllers;

/// <summary>
/// 供使用者透過 Web 介面維護搬移設定並觸發搬移流程的 MVC 控制器。
/// </summary>
public class ArchiveSettingsController : Controller
{
    private readonly IArchiveSettingRepository _repository;
    private readonly ArchiveExecutionService _executionService;
    private readonly ILogger<ArchiveSettingsController> _logger;

    /// <summary>
    /// 建構子注入 Repository 與搬移服務。
    /// </summary>
    public ArchiveSettingsController(
        IArchiveSettingRepository repository,
        ArchiveExecutionService executionService,
        ILogger<ArchiveSettingsController> logger)
    {
        _repository = repository;
        _executionService = executionService;
        _logger = logger;
    }

    /// <summary>
    /// 讀取所有設定並顯示設定頁面。
    /// </summary>
    [HttpGet]
    public async Task<IActionResult> Index(CancellationToken cancellationToken)
    {
        var vm = await BuildPageViewModelAsync(null, cancellationToken);
        return View(vm);
    }

    /// <summary>
    /// 讀取單筆設定並帶入編輯表單。
    /// </summary>
    [HttpGet]
    public async Task<IActionResult> Edit(int id, CancellationToken cancellationToken)
    {
        var target = await _repository.GetByIdAsync(id, cancellationToken);
        if (target is null)
        {
            TempData["Error"] = "找不到指定的設定";
            return RedirectToAction(nameof(Index));
        }

        var vm = await BuildPageViewModelAsync(target, cancellationToken);
        return View("Index", vm);
    }

    /// <summary>
    /// 新增或更新設定，儲存至資料庫後回到列表。
    /// </summary>
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Save(ArchiveSettingsPageViewModel model, CancellationToken cancellationToken)
    {
        if (!ModelState.IsValid)
        {
            model.ExistingSettings = await _repository.GetAllAsync(cancellationToken);
            return View("Index", model);
        }

        var form = model.Form;
        var entity = new ArchiveSetting
        {
            Id = form.Id ?? 0,
            SourceConnectionName = form.SourceConnectionName.Trim(),
            TargetConnectionName = form.TargetConnectionName.Trim(),
            TableName = form.TableName.Trim(),
            DateColumn = form.DateColumn.Trim(),
            PrimaryKeyColumn = form.PrimaryKeyColumn.Trim(),
            OnlineRetentionMonths = form.OnlineRetentionMonths,
            HistoryRetentionMonths = form.HistoryRetentionMonths,
            BatchSize = form.BatchSize,
            CsvEnabled = form.CsvEnabled,
            CsvRootFolder = form.CsvRootFolder.Trim(),
            Enabled = form.Enabled
        };

        try
        {
            await _repository.UpsertAsync(entity, cancellationToken);
            TempData["Success"] = $"{entity.TableName} 設定已儲存";
            _logger.LogInformation("使用者儲存設定：{Table}", entity.TableName);
        }
        catch (Exception ex)
        {
            TempData["Error"] = $"儲存設定失敗：{ex.GetBaseException().Message}";
            _logger.LogError(ex, "儲存設定失敗 {Table}", entity.TableName);
        }

        return RedirectToAction(nameof(Index));
    }

    /// <summary>
    /// 手動觸發一次搬移流程並回傳執行結果。
    /// </summary>
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Run(CancellationToken cancellationToken)
    {
        try
        {
            var result = await _executionService.RunOnceAsync(cancellationToken);
            var existing = await _repository.GetAllAsync(cancellationToken);

            var vm = new ArchiveSettingsPageViewModel
            {
                ExistingSettings = existing,
                ExecutionMessages = result.Messages,
                ExecutionSucceeded = result.Succeeded
            };

            return View("Index", vm);
        }
        catch (Exception ex)
        {
            var existing = await _repository.GetAllAsync(cancellationToken);
            var vm = new ArchiveSettingsPageViewModel
            {
                ExistingSettings = existing,
                ExecutionMessages = new[] { $"執行失敗：{ex.GetBaseException().Message}" },
                ExecutionSucceeded = false
            };

            _logger.LogError(ex, "手動執行搬移時發生未處理例外");
            return View("Index", vm);
        }
    }

    /// <summary>
    /// 刪除指定設定並返回列表。
    /// </summary>
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Delete(int id, CancellationToken cancellationToken)
    {
        try
        {
            await _repository.DeleteAsync(id, cancellationToken);
            TempData["Success"] = "設定已刪除";
        }
        catch (Exception ex)
        {
            TempData["Error"] = $"刪除設定失敗：{ex.GetBaseException().Message}";
            _logger.LogError(ex, "刪除設定失敗 {Id}", id);
        }

        return RedirectToAction(nameof(Index));
    }

    /// <summary>
    /// 建立設定頁面 ViewModel，並可帶入正在編輯的設定以回填表單。
    /// </summary>
    /// <param name="editing">若為編輯模式則為目標設定，否則為 null。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    private async Task<ArchiveSettingsPageViewModel> BuildPageViewModelAsync(ArchiveSetting? editing, CancellationToken cancellationToken)
    {
        var existing = await _repository.GetAllAsync(cancellationToken);

        return new ArchiveSettingsPageViewModel
        {
            ExistingSettings = existing,
            Form = editing is null
                ? new ArchiveSettingInputModel()
                : new ArchiveSettingInputModel
                {
                    Id = editing.Id,
                    SourceConnectionName = editing.SourceConnectionName,
                    TargetConnectionName = editing.TargetConnectionName,
                    TableName = editing.TableName,
                    DateColumn = editing.DateColumn,
                    PrimaryKeyColumn = editing.PrimaryKeyColumn,
                    OnlineRetentionMonths = editing.OnlineRetentionMonths,
                    HistoryRetentionMonths = editing.HistoryRetentionMonths,
                    BatchSize = editing.BatchSize,
                    CsvEnabled = editing.CsvEnabled,
                    CsvRootFolder = editing.CsvRootFolder,
                    Enabled = editing.Enabled
                }
        };
    }
}
