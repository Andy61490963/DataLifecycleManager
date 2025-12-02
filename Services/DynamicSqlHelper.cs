using System.Collections.Concurrent;
using System.Dynamic;
using System.Text;

namespace DataLifecycleManager.Services;

/// <summary>
/// 針對動態物件批次建置 SQL 語句與欄位資訊的輔助類別。
/// </summary>
public static class DynamicSqlHelper
{
    private static readonly ConcurrentDictionary<string, string> InsertSqlCache = new();

    /// <summary>
    /// 從 Expando 物件取得欄位清單，保持原始大小寫。
    /// </summary>
    /// <param name="row">任意資料列。</param>
    /// <returns>欄位名稱集合。</returns>
    public static IReadOnlyList<string> GetColumnNames(object row)
    {
        if (row is not ExpandoObject expando)
        {
            throw new InvalidOperationException("僅支援動態資料列。");
        }

        return ((IDictionary<string, object?>)expando).Keys.ToList();
    }

    /// <summary>
    /// 建立具備唯一性檢查的 Insert 語句，避免重複搬移造成主鍵衝突。
    /// </summary>
    /// <param name="tableName">目標資料表。</param>
    /// <param name="columns">欄位清單。</param>
    /// <param name="primaryKeyColumn">主鍵欄位名稱。</param>
    /// <returns>完成的 SQL 字串。</returns>
    public static string BuildInsertSql(string tableName, IReadOnlyList<string> columns, string primaryKeyColumn)
    {
        var cacheKey = $"{tableName}:{primaryKeyColumn}:{string.Join(',', columns)}";
        return InsertSqlCache.GetOrAdd(cacheKey, _ =>
        {
            var columnList = string.Join(",", columns.Select(Bracket));
            var parameterList = string.Join(",", columns.Select(c => "@" + c));

            var builder = new StringBuilder();
            builder.Append($"INSERT INTO {Bracket(tableName)} ({columnList}) ");
            builder.Append($"SELECT {parameterList} ");
            builder.Append("WHERE NOT EXISTS (SELECT 1 FROM ");
            builder.Append(Bracket(tableName));
            builder.Append($" WHERE {Bracket(primaryKeyColumn)} = @{primaryKeyColumn});");

            return builder.ToString();
        });
    }

    /// <summary>
    /// 建立刪除 SQL，使用 IN 條件批次刪除。
    /// </summary>
    /// <param name="tableName">資料表名稱。</param>
    /// <param name="primaryKeyColumn">主鍵欄位。</param>
    /// <returns>刪除語句。</returns>
    public static string BuildDeleteSql(string tableName, string primaryKeyColumn)
    {
        return $"DELETE FROM {Bracket(tableName)} WHERE {Bracket(primaryKeyColumn)} IN @Ids";
    }

    private static string Bracket(string value) => $"[{value}]";
}
