namespace DataLifecycleManager.Models;

/// <summary>
/// 資料表搬移的設定資料模型，對應資料庫設定表的欄位。
/// </summary>
public class ArchiveSetting
{
    /// <summary>設定主鍵。</summary>
    public int Id { get; set; }

    /// <summary>來源資料庫的連線名稱。</summary>
    public required string SourceConnectionName { get; set; }

    /// <summary>目標資料庫的連線名稱。</summary>
    public required string TargetConnectionName { get; set; }

    /// <summary>欲搬移的資料表名稱。</summary>
    public required string TableName { get; set; }

    /// <summary>用來判斷日期門檻的欄位名稱。</summary>
    public required string DateColumn { get; set; }

    /// <summary>用來做冪等與刪除的主鍵欄位名稱。</summary>
    public required string PrimaryKeyColumn { get; set; }

    /// <summary>線上庫保留的最後日期，早於此日期的資料會搬移到歷史庫。</summary>
    public DateTime OnlineRetentionDate { get; set; }

    /// <summary>歷史庫保留的最後日期，早於此日期的資料會匯出 CSV 並刪除。</summary>
    public DateTime HistoryRetentionDate { get; set; }

    /// <summary>單批處理筆數。</summary>
    public int BatchSize { get; set; }

    /// <summary>是否啟用 CSV 匯出。</summary>
    public bool CsvEnabled { get; set; }

    /// <summary>CSV 存放根目錄。</summary>
    public required string CsvRootFolder { get; set; }

    /// <summary>是否啟用此搬移設定。</summary>
    public bool Enabled { get; set; }
}
