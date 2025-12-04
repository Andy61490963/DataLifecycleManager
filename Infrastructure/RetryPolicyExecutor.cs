using DataLifecycleManager.Configuration;
using Microsoft.Data.SqlClient;
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

    public async Task ExecuteAsync(string operationName, Func<Task> action, CancellationToken cancellationToken)
    {
        if (!_settings.Enabled)
        {
            // 不啟用 retry 就直接執行一次
            await action();
            return;
        }

        var attempts = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                attempts++;
                await action();
                return;
            }
            catch (Exception ex) when (ShouldRetry(ex, attempts, cancellationToken))
            {
                _logger.LogWarning(
                    ex,
                    "{Operation} 失敗，第 {Attempt}/{Max} 次重試前等待 {Delay}s",
                    operationName,
                    attempts,
                    _settings.MaxRetryCount,
                    _settings.RetryDelaySeconds
                );

                if (_settings.RetryDelaySeconds > 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(_settings.RetryDelaySeconds), cancellationToken);
                }
            }
        }
    }

    /// <summary>
    /// 判斷是否應該重試：
    /// - 不超過最大重試次數
    /// - 非取消錯誤
    /// - 只針對特定 transient 錯誤（例如 deadlock）重試
    /// - **SQL Timeout（-2）不重試，直接讓上層處理**
    /// </summary>
    private bool ShouldRetry(Exception exception, int attempts, CancellationToken cancellationToken)
    {
        // 已經到達最大重試次數 → 不再重試
        if (attempts >= _settings.MaxRetryCount)
        {
            return false;
        }

        // 已經被取消 → 不重試
        if (cancellationToken.IsCancellationRequested || exception is OperationCanceledException)
        {
            return false;
        }

        // 只對特定 SqlException 做重試判斷
        if (exception is SqlException sqlEx)
        {
            // ❌ -2：Execution Timeout Expired → 視為「查詢太慢 / 結構問題」，不重試
            if (sqlEx.Number == -2)
            {
                return false;
            }

            // ✅ 1205：Deadlock → 很典型的 transient error，適合重試一次
            if (sqlEx.Number == 1205)
            {
                return true;
            }

            // 這裡可以依需求再加其他你想當 transient 的 error number
            // e.g. 4060, 10928, 10929, 40501, 40197 ...（多半是連線 / throttle 類）
            return false;
        }

        // 其他類型 Exception 預設不重試，避免隱藏程式邏輯 bug
        return false;
    }
       
}
