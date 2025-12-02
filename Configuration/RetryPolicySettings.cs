using System.ComponentModel.DataAnnotations;

namespace DataLifecycleManager.Configuration;

/// <summary>
/// 重試策略設定值，控制單次搬移的容錯行為。
/// </summary>
public class RetryPolicySettings
{
    /// <summary>是否啟用重試。</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>最大重試次數。</summary>
    [Range(0, 10)]
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>重試間隔秒數。</summary>
    [Range(0, 300)]
    public int RetryDelaySeconds { get; set; } = 5;
}
