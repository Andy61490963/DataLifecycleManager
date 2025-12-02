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
    /// <param name="connectionNameOrString">連線名稱或完整連線字串。</param>
    /// <returns>尚未開啟的連線物件。</returns>
    public SqlConnection CreateConnection(string connectionNameOrString)
    {
        if (string.IsNullOrWhiteSpace(connectionNameOrString))
        {
            throw new InvalidOperationException("連線名稱或連線字串不可為空白。");
        }

        var configured = _configuration.GetConnectionString(connectionNameOrString);
        var connectionString = string.IsNullOrWhiteSpace(configured)
            ? connectionNameOrString
            : configured;

        return new SqlConnection(connectionString);
    }
}
