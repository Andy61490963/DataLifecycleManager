using Dapper;
using DataLifecycleManager.Domain;
using DataLifecycleManager.Infrastructure;
using Microsoft.Extensions.Configuration;

namespace DataLifecycleManager.Repositories;

/// <summary>
/// 以 Dapper 讀取資料庫中的歸檔任務設定與重試策略。
/// </summary>
public class ArchiveJobRepository : IArchiveJobRepository
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly string _managementConnectionName;

    /// <summary>
    /// 建構子注入連線工廠與組態來源。
    /// </summary>
    /// <param name="configuration">組態取得管理庫連線名稱。</param>
    /// <param name="connectionFactory">SQL 連線工廠。</param>
    public ArchiveJobRepository(IConfiguration configuration, SqlConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
        _managementConnectionName = configuration.GetValue<string>("ManagementDbConnection") ?? "Management";
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ArchiveJobDefinition>> GetEnabledJobsAsync(CancellationToken cancellationToken)
    {
        const string sql = @"SELECT j.Id,
                                      j.JobName,
                                      j.TableName,
                                      j.SourceConnection,
                                      j.TargetConnection,
                                      j.DateColumn,
                                      j.PrimaryKeyColumn,
                                      j.OnlineRetentionMonths,
                                      j.HistoryRetentionMonths,
                                      j.BatchSize,
                                      j.CsvEnabled,
                                      j.CsvRootFolder,
                                      j.Enabled,
                                      rp.RetryCount,
                                      rp.RetryDelaySeconds
                               FROM dbo.ArchiveJob j
                               LEFT JOIN dbo.ArchiveJobRetryPolicy rp ON rp.JobId = j.Id
                               WHERE j.Enabled = 1";

        await using var connection = _connectionFactory.CreateConnection(_managementConnectionName);
        await connection.OpenAsync(cancellationToken);

        var jobs = await connection.QueryAsync(sql);

        return jobs
            .Select(row => MapToDefinition((IDictionary<string, object?>)row))
            .ToList();
    }

    private static ArchiveJobDefinition MapToDefinition(IDictionary<string, object?> row)
    {
        return new ArchiveJobDefinition
        {
            Id = (int)row[nameof(ArchiveJobDefinition.Id)],
            JobName = Convert.ToString(row[nameof(ArchiveJobDefinition.JobName)]) ?? string.Empty,
            TableName = Convert.ToString(row[nameof(ArchiveJobDefinition.TableName)]) ?? string.Empty,
            SourceConnection = Convert.ToString(row[nameof(ArchiveJobDefinition.SourceConnection)]) ?? string.Empty,
            TargetConnection = Convert.ToString(row[nameof(ArchiveJobDefinition.TargetConnection)]) ?? string.Empty,
            DateColumn = Convert.ToString(row[nameof(ArchiveJobDefinition.DateColumn)]) ?? string.Empty,
            PrimaryKeyColumn = Convert.ToString(row[nameof(ArchiveJobDefinition.PrimaryKeyColumn)]) ?? string.Empty,
            OnlineRetentionMonths = Convert.ToInt32(row[nameof(ArchiveJobDefinition.OnlineRetentionMonths)]),
            HistoryRetentionMonths = Convert.ToInt32(row[nameof(ArchiveJobDefinition.HistoryRetentionMonths)]),
            BatchSize = Convert.ToInt32(row[nameof(ArchiveJobDefinition.BatchSize)]),
            CsvEnabled = Convert.ToBoolean(row[nameof(ArchiveJobDefinition.CsvEnabled)]),
            CsvRootFolder = row[nameof(ArchiveJobDefinition.CsvRootFolder)] as string,
            Enabled = Convert.ToBoolean(row[nameof(ArchiveJobDefinition.Enabled)]),
            RetryPolicy = BuildRetry(row)
        };
    }

    private static RetryPolicySettings? BuildRetry(IDictionary<string, object?> row)
    {
        if (row["RetryCount"] is null || row["RetryDelaySeconds"] is null)
        {
            return null;
        }

        return new RetryPolicySettings
        {
            Enabled = true,
            MaxRetryCount = Convert.ToInt32(row["RetryCount"]),
            RetryDelaySeconds = Convert.ToInt32(row["RetryDelaySeconds"])
        };
    }
}
