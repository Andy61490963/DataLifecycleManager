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
        var existing = await _repository.GetAllAsync(cancellationToken);
        var vm = new ArchiveSettingsPageViewModel
        {
            ExistingSettings = existing
        };

        return View(vm);
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
            CsvRootFolder = form.CsvRootFolder.Trim()
        };

        await _repository.UpsertAsync(entity, cancellationToken);
        TempData["Success"] = $"{entity.TableName} 設定已儲存";
        _logger.LogInformation("使用者儲存設定：{Table}", entity.TableName);

        return RedirectToAction(nameof(Index));
    }

    /// <summary>
    /// 手動觸發一次搬移流程並回傳執行結果。
    /// </summary>
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Run(CancellationToken cancellationToken)
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
}
