using DataLifecycleManager.Services;

namespace DataLifecycleManager;

/// <summary>
/// 背景常駐排程服務，負責按日觸發歸檔與匯出流程。
/// </summary>
public class Worker : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromDays(1);
    private readonly ArchiveCoordinator _coordinator;
    private readonly ILogger<Worker> _logger;

    /// <summary>
    /// 建構子注入流程協調器與紀錄器。
    /// </summary>
    /// <param name="coordinator">歸檔協調器。</param>
    /// <param name="logger">紀錄器。</param>
    public Worker(ArchiveCoordinator coordinator, ILogger<Worker> logger)
    {
        _coordinator = coordinator;
        _logger = logger;
    }

    /// <summary>
    /// 以週期方式執行排程，並確保例外被捕捉記錄。
    /// </summary>
    /// <param name="stoppingToken">取消權杖。</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("資料歸檔排程啟動");
        await RunSafelyAsync(stoppingToken);

        using var timer = new PeriodicTimer(Interval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            await RunSafelyAsync(stoppingToken);
        }
    }

    private async Task RunSafelyAsync(CancellationToken stoppingToken)
    {
        try
        {
            await _coordinator.RunOnceAsync(stoppingToken);
        }
        catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogError(ex, "排程執行發生例外，將於下次週期再嘗試");
        }
    }
}
