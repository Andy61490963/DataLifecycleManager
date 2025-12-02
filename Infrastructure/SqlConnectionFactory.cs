using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace DataLifecycleManager.Infrastructure;

/// <summary>
/// 依照設定的連線字串名稱建立 SqlConnection，統一管理連線存取邏輯。
/// </summary>
public class SqlConnectionFactory
{
    private readonly IConfiguration _configuration;

    /// <summary>
    /// 建構子注入組態以取得連線字串。
    /// </summary>
    /// <param name="configuration">應用程式組態來源。</param>
    public SqlConnectionFactory(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    /// <summary>
    /// 建立新的資料庫連線。
    /// </summary>
    /// <param name="connectionName">連線字串名稱。</param>
    /// <returns>尚未開啟的連線物件。</returns>
    public SqlConnection CreateConnection(string connectionName)
    {
        var connectionString = _configuration.GetConnectionString(connectionName);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException($"Connection string '{connectionName}' 未設定。");
        }

        return new SqlConnection(connectionString);
    }
}
