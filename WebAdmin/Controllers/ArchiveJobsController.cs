using System.Data;
using Dapper;
using DataLifecycleManager.Domain;
using DataLifecycleManager.Repositories;
using DataLifecycleManager.WebAdmin.Models;
using Microsoft.AspNetCore.Mvc;

namespace DataLifecycleManager.WebAdmin.Controllers;

/// <summary>
/// 提供歸檔任務的 CRUD 後台管理控制器。
/// </summary>
public class ArchiveJobsController : Controller
{
    private readonly IArchiveJobRepository _repository;
    private readonly SqlConnectionFactory _connectionFactory;

    /// <summary>
    /// 建構子注入資料存取元件。
    /// </summary>
    public ArchiveJobsController(IArchiveJobRepository repository, SqlConnectionFactory connectionFactory)
    {
        _repository = repository;
        _connectionFactory = connectionFactory;
    }

    /// <summary>
    /// 列出所有啟用的任務設定。
    /// </summary>
    public async Task<IActionResult> Index(CancellationToken cancellationToken)
    {
        var jobs = await _repository.GetEnabledJobsAsync(cancellationToken);
        return View(jobs);
    }

    /// <summary>
    /// 顯示新增畫面。
    /// </summary>
    public IActionResult Create()
    {
        return View(new ArchiveJobViewModel());
    }

    /// <summary>
    /// 新增任務設定。
    /// </summary>
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ArchiveJobViewModel model, CancellationToken cancellationToken)
    {
        if (!ModelState.IsValid)
        {
            return View(model);
        }

        await using var connection = _connectionFactory.CreateConnection("Management");
        var insertDefinition = new CommandDefinition(
            "INSERT INTO dbo.ArchiveJob (JobName, TableName, SourceConnection, TargetConnection, DateColumn, PrimaryKeyColumn, OnlineRetentionMonths, HistoryRetentionMonths, BatchSize, CsvEnabled, CsvRootFolder, Enabled)\n             VALUES (@JobName, @TableName, @SourceConnection, @TargetConnection, @DateColumn, @PrimaryKeyColumn, @OnlineRetentionMonths, @HistoryRetentionMonths, @BatchSize, @CsvEnabled, @CsvRootFolder, @Enabled);",
            model,
            cancellationToken: cancellationToken);
        await connection.ExecuteAsync(insertDefinition);

        await UpsertRetryAsync(connection, model, cancellationToken);
        return RedirectToAction(nameof(Index));
    }

    /// <summary>
    /// 編輯畫面。
    /// </summary>
    public async Task<IActionResult> Edit(int id, CancellationToken cancellationToken)
    {
        await using var connection = _connectionFactory.CreateConnection("Management");
        var job = await connection.QuerySingleAsync<ArchiveJobViewModel>(
            new CommandDefinition("SELECT * FROM dbo.ArchiveJob WHERE Id=@Id", new { Id = id }, cancellationToken: cancellationToken));
        var retry = await connection.QuerySingleOrDefaultAsync<(int RetryCount, int RetryDelaySeconds)?>(
            new CommandDefinition("SELECT RetryCount, RetryDelaySeconds FROM dbo.ArchiveJobRetryPolicy WHERE JobId=@Id", new { Id = id }, cancellationToken: cancellationToken));

        if (retry.HasValue)
        {
            job.RetryCount = retry.Value.RetryCount;
            job.RetryDelaySeconds = retry.Value.RetryDelaySeconds;
        }

        return View(job);
    }

    /// <summary>
    /// 更新任務設定。
    /// </summary>
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, ArchiveJobViewModel model, CancellationToken cancellationToken)
    {
        if (!ModelState.IsValid)
        {
            return View(model);
        }

        await using var connection = _connectionFactory.CreateConnection("Management");
        var updateDefinition = new CommandDefinition(
            "UPDATE dbo.ArchiveJob SET JobName=@JobName, TableName=@TableName, SourceConnection=@SourceConnection, TargetConnection=@TargetConnection, DateColumn=@DateColumn, PrimaryKeyColumn=@PrimaryKeyColumn, OnlineRetentionMonths=@OnlineRetentionMonths, HistoryRetentionMonths=@HistoryRetentionMonths, BatchSize=@BatchSize, CsvEnabled=@CsvEnabled, CsvRootFolder=@CsvRootFolder, Enabled=@Enabled WHERE Id=@Id",
            model,
            cancellationToken: cancellationToken);
        await connection.ExecuteAsync(updateDefinition);

        await UpsertRetryAsync(connection, model, cancellationToken);
        return RedirectToAction(nameof(Index));
    }

    /// <summary>
    /// 刪除任務設定。
    /// </summary>
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Delete(int id, CancellationToken cancellationToken)
    {
        await using var connection = _connectionFactory.CreateConnection("Management");
        var deleteDefinition = new CommandDefinition(
            "DELETE FROM dbo.ArchiveJob WHERE Id=@Id",
            new { Id = id },
            cancellationToken: cancellationToken);
        await connection.ExecuteAsync(deleteDefinition);
        return RedirectToAction(nameof(Index));
    }

    /// <summary>
    /// 新增或更新重試策略設定，未填值時即刪除原有策略。
    /// </summary>
    private static async Task UpsertRetryAsync(IDbConnection connection, ArchiveJobViewModel model, CancellationToken cancellationToken)
    {
        if (model.RetryCount is null || model.RetryDelaySeconds is null)
        {
            await connection.ExecuteAsync("DELETE FROM dbo.ArchiveJobRetryPolicy WHERE JobId=@Id", new { model.Id });
            return;
        }

        await connection.ExecuteAsync(
            "MERGE dbo.ArchiveJobRetryPolicy AS target USING (SELECT @Id AS JobId) AS source ON target.JobId = source.JobId WHEN MATCHED THEN UPDATE SET RetryCount=@RetryCount, RetryDelaySeconds=@RetryDelaySeconds WHEN NOT MATCHED THEN INSERT (JobId, RetryCount, RetryDelaySeconds) VALUES (@Id, @RetryCount, @RetryDelaySeconds);",
            new { model.Id, model.RetryCount, model.RetryDelaySeconds });
    }
}
