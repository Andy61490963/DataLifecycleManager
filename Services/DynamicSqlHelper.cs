using System.Collections.Concurrent;
using System.Text;

namespace DataLifecycleManager.Services;

/// <summary>
/// 針對動態物件批次建置 SQL 語句與欄位資訊的輔助類別。
/// </summary>
public static class DynamicSqlHelper
{
    private static readonly ConcurrentDictionary<string, string> InsertSqlCache = new();

    /// <summary>
    /// 從欄位字典取得欄位清單，保持原始大小寫。
    /// </summary>
    /// <param name="row">任意資料列的欄位字典。</param>
    /// <returns>欄位名稱集合。</returns>
    public static IReadOnlyList<string> GetColumnNames(
        IReadOnlyDictionary<string, object?> row)
    {
        // 這裡直接回傳 Keys，比原本判斷 Expando 更通用
        return row.Keys.ToList();
    }

    public static string BuildInsertSql(
        string tableName,
        IReadOnlyList<string> columns,
        string primaryKeyColumn)
    {
        var columnList    = string.Join(",", columns.Select(Bracket));
        var parameterList = string.Join(",", columns.Select(c => "@" + c));

        return $"""
                INSERT INTO {Bracket(tableName)} ({columnList})
                SELECT {parameterList}
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {Bracket(tableName)}
                    WHERE {Bracket(primaryKeyColumn)} = @{primaryKeyColumn}
                );
                """;
    }

    public static string BuildDeleteSql(string tableName, string primaryKeyColumn)
    {
        return $"""
                DELETE FROM {Bracket(tableName)}
                WHERE {Bracket(primaryKeyColumn)} IN @Ids;
                """;
    }

    private static string Bracket(string value) => $"[{value}]";
}

