using Dapper;
using DataLifecycleManager.Infrastructure;
using DataLifecycleManager.Models;

namespace DataLifecycleManager.Repositories;

/// <summary>
/// 以 Dapper 操作設定表的 Repository 實作。
/// </summary>
public class ArchiveSettingRepository : IArchiveSettingRepository
{
    private const string ConfigurationConnectionName = "ConfigurationDb";
    private readonly SqlConnectionFactory _connectionFactory;

    /// <summary>
    /// 建構子注入連線工廠，統一取得設定資料庫連線。
    /// </summary>
    /// <param name="connectionFactory">連線工廠。</param>
    public ArchiveSettingRepository(SqlConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ArchiveSetting>> GetAllAsync(CancellationToken cancellationToken)
    {
        const string sql = "SELECT Id, SourceConnectionName, TargetConnectionName, TableName, DateColumn, PrimaryKeyColumn, OnlineRetentionMonths, HistoryRetentionMonths, BatchSize, CsvEnabled, CsvRootFolder, Enabled FROM dbo.ArchiveSettings ORDER BY TableName";

        await using var connection = _connectionFactory.CreateConnection(ConfigurationConnectionName);
        await connection.OpenAsync(cancellationToken);

        var result = await connection.QueryAsync<ArchiveSetting>(new CommandDefinition(sql, cancellationToken: cancellationToken));
        return result.ToList();
    }

    /// <inheritdoc />
    public async Task<ArchiveSetting?> GetByIdAsync(int id, CancellationToken cancellationToken)
    {
        const string sql = "SELECT Id, SourceConnectionName, TargetConnectionName, TableName, DateColumn, PrimaryKeyColumn, OnlineRetentionMonths, HistoryRetentionMonths, BatchSize, CsvEnabled, CsvRootFolder, Enabled FROM dbo.ArchiveSettings WHERE Id = @Id";

        await using var connection = _connectionFactory.CreateConnection(ConfigurationConnectionName);
        await connection.OpenAsync(cancellationToken);

        var command = new CommandDefinition(sql, new { Id = id }, cancellationToken: cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<ArchiveSetting>(command);
    }

    /// <inheritdoc />
    public async Task<int> UpsertAsync(ArchiveSetting setting, CancellationToken cancellationToken)
    {
        const string insertSql = """
            INSERT INTO dbo.ArchiveSettings (
                SourceConnectionName,
                TargetConnectionName,
                TableName,
                DateColumn,
                PrimaryKeyColumn,
                OnlineRetentionMonths,
                HistoryRetentionMonths,
                BatchSize,
                CsvEnabled,
                CsvRootFolder,
                Enabled)
            VALUES (
                @SourceConnectionName,
                @TargetConnectionName,
                @TableName,
                @DateColumn,
                @PrimaryKeyColumn,
                @OnlineRetentionMonths,
                @HistoryRetentionMonths,
                @BatchSize,
                @CsvEnabled,
                @CsvRootFolder,
                @Enabled);
            SELECT CAST(SCOPE_IDENTITY() AS INT);
            """;

        const string updateSql = """
            UPDATE dbo.ArchiveSettings
            SET SourceConnectionName    = @SourceConnectionName,
                TargetConnectionName    = @TargetConnectionName,
                TableName               = @TableName,
                DateColumn              = @DateColumn,
                PrimaryKeyColumn        = @PrimaryKeyColumn,
                OnlineRetentionMonths   = @OnlineRetentionMonths,
                HistoryRetentionMonths  = @HistoryRetentionMonths,
                BatchSize               = @BatchSize,
                CsvEnabled              = @CsvEnabled,
                CsvRootFolder           = @CsvRootFolder,
                Enabled                 = @Enabled
            WHERE Id = @Id;
            SELECT @Id;
            """;

        var sql = setting.Id <= 0 ? insertSql : updateSql;

        await using var connection = _connectionFactory.CreateConnection(ConfigurationConnectionName);
        await connection.OpenAsync(cancellationToken);

        var command = new CommandDefinition(sql, setting, cancellationToken: cancellationToken);
        var id = await connection.ExecuteScalarAsync<int>(command);
        return id;
    }

    /// <inheritdoc />
    public async Task DeleteAsync(int id, CancellationToken cancellationToken)
    {
        const string sql = "DELETE FROM dbo.ArchiveSettings WHERE Id = @Id";

        await using var connection = _connectionFactory.CreateConnection(ConfigurationConnectionName);
        await connection.OpenAsync(cancellationToken);

        var command = new CommandDefinition(sql, new { Id = id }, cancellationToken: cancellationToken);
        await connection.ExecuteAsync(command);
    }
}
