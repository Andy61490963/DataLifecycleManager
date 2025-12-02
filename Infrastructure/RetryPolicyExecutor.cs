using DataLifecycleManager.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DataLifecycleManager.Infrastructure;

/// <summary>
/// 提供具延遲的簡易重試封裝，避免短暫錯誤造成整批中斷。
/// </summary>
public class RetryPolicyExecutor
{
    private readonly ILogger<RetryPolicyExecutor> _logger;
    private readonly RetryPolicySettings _settings;

    /// <summary>
    /// 初始化重試封裝元件。
    /// </summary>
    /// <param name="logger">紀錄器。</param>
    /// <param name="options">包含重試設定的組態來源。</param>
    public RetryPolicyExecutor(ILogger<RetryPolicyExecutor> logger, IOptions<RetryPolicySettings> options)
    {
        _logger = logger;
        _settings = options.Value;
    }

    /// <summary>
    /// 執行具有重試的非同步作業。
    /// </summary>
    /// <param name="operationName">作業名稱，便於紀錄。</param>
    /// <param name="action">實際需要執行的委派。</param>
    /// <param name="cancellationToken">取消權杖。</param>
    public async Task ExecuteAsync(string operationName, Func<Task> action, CancellationToken cancellationToken)
    {
        if (!_settings.Enabled)
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
            catch (Exception ex) when (attempts <= _settings.MaxRetryCount)
            {
                _logger.LogWarning(ex, "{Operation} 失敗，第 {Attempt}/{Max} 次重試前等待 {Delay}s", operationName, attempts, _settings.MaxRetryCount, _settings.RetryDelaySeconds);
                if (_settings.RetryDelaySeconds > 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(_settings.RetryDelaySeconds), cancellationToken);
                }
            }
        }
    }
}
