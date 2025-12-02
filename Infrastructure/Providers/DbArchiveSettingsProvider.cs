using DataLifecycleManager.Configuration;
using DataLifecycleManager.Domain;
using Microsoft.Extensions.Options;

namespace DataLifecycleManager.Infrastructure.Providers;

/// <summary>
/// 由資料庫載入歸檔設定並與全域預設合併的 Provider。
/// </summary>
public class DbArchiveSettingsProvider : IArchiveSettingsProvider
{
    private readonly ArchiveDefaultsOptions _defaults;
    private readonly IArchiveJobRepository _repository;

    /// <summary>
    /// 建構子注入預設值與資料來源。
    /// </summary>
    /// <param name="defaults">全域預設設定。</param>
    /// <param name="repository">任務設定存取庫。</param>
    public DbArchiveSettingsProvider(IOptions<ArchiveDefaultsOptions> defaults, IArchiveJobRepository repository)
    {
        _defaults = defaults.Value;
        _repository = repository;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ArchiveJobRuntimeSettings>> GetJobsAsync(CancellationToken cancellationToken)
    {
        if (!_defaults.Enabled)
        {
            return Array.Empty<ArchiveJobRuntimeSettings>();
        }

        var jobs = await _repository.GetEnabledJobsAsync(cancellationToken);

        return jobs
            .Where(j => j.Enabled)
            .Select(MergeDefaults)
            .ToList();
    }

    private ArchiveJobRuntimeSettings MergeDefaults(ArchiveJobDefinition job)
    {
        var retry = job.RetryPolicy ?? _defaults.RetryPolicy;

        return new ArchiveJobRuntimeSettings
        {
            Id = job.Id,
            JobName = job.JobName,
            TableName = job.TableName,
            SourceConnection = job.SourceConnection,
            TargetConnection = job.TargetConnection,
            DateColumn = job.DateColumn,
            PrimaryKeyColumn = job.PrimaryKeyColumn,
            OnlineRetentionMonths = CoalescePositive(job.OnlineRetentionMonths, _defaults.DefaultOnlineRetentionMonths),
            HistoryRetentionMonths = CoalescePositive(job.HistoryRetentionMonths, _defaults.DefaultHistoryRetentionMonths),
            BatchSize = CoalescePositive(job.BatchSize, _defaults.DefaultBatchSize),
            CsvEnabled = job.CsvEnabled && _defaults.Csv.Enabled,
            CsvRootFolder = string.IsNullOrWhiteSpace(job.CsvRootFolder) ? _defaults.Csv.RootFolder : job.CsvRootFolder,
            RetryPolicy = new RetryPolicySettings
            {
                Enabled = retry.Enabled,
                MaxRetryCount = retry.MaxRetryCount,
                RetryDelaySeconds = retry.RetryDelaySeconds
            }
        };
    }

    private static int CoalescePositive(int candidate, int fallback) => candidate > 0 ? candidate : fallback;
}
