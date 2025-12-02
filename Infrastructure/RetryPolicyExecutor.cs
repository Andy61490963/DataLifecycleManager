using DataLifecycleManager.Domain;
using Microsoft.Extensions.Logging;

namespace DataLifecycleManager.Infrastructure;

/// <summary>
/// 提供具延遲的簡易重試封裝，避免短暫錯誤造成整批中斷。
/// </summary>
public class RetryPolicyExecutor
{
    private readonly ILogger<RetryPolicyExecutor> _logger;

    /// <summary>
    /// 初始化重試封裝元件。
    /// </summary>
    /// <param name="logger">紀錄器。</param>
    public RetryPolicyExecutor(ILogger<RetryPolicyExecutor> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// 執行具有重試的非同步作業。
    /// </summary>
    /// <param name="operationName">作業名稱，便於紀錄。</param>
    /// <param name="policy">重試策略設定。</param>
    /// <param name="action">實際需要執行的委派。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    public async Task ExecuteAsync(
        string operationName,
        RetryPolicySettings policy,
        Func<Task> action,
        CancellationToken cancellationToken)
    {
        if (!policy.Enabled)
        {
            await action();
            return;
        }

        var attempts = 0;
        while (true)
        {
            try
            {
                attempts++;
                await action();
                return;
            }
            catch (Exception ex) when (attempts <= policy.MaxRetryCount)
            {
                _logger.LogWarning(
                    ex,
                    "{Operation} 失敗，第 {Attempt}/{Max} 次重試前等待 {Delay}s",
                    operationName,
                    attempts,
                    policy.MaxRetryCount,
                    policy.RetryDelaySeconds);

                if (policy.RetryDelaySeconds > 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(policy.RetryDelaySeconds), cancellationToken);
                }
            }
        }
    }
}
