namespace DataLifecycleManager.Configuration;

/// <summary>
/// Serilog 使用的設定值，允許透過 appsettings 控制輸出管道與等級。
/// </summary>
public class AppLoggingOptions
{
    /// <summary>應用程式名稱識別。</summary>
    public string ApplicationName { get; set; } = "DataLifecycleManager";

    /// <summary>最小記錄等級字串（verbose/debug/information/warning/error/fatal）。</summary>
    public string MinimumLevel { get; set; } = "Information";

    /// <summary>檔案輸出設定。</summary>
    public FileLoggingOptions File { get; set; } = new();

    /// <summary>Seq 輸出設定。</summary>
    public SeqLoggingOptions Seq { get; set; } = new();
}

/// <summary>檔案寫入設定。</summary>
public class FileLoggingOptions
{
    /// <summary>是否啟用檔案輸出。</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>保留天數。</summary>
    public int RetainDays { get; set; } = 14;

    /// <summary>檔案大小上限 (MB)。</summary>
    public int FileSizeLimitMB { get; set; } = 100;
}

/// <summary>Seq 系統輸出設定。</summary>
public class SeqLoggingOptions
{
    /// <summary>是否啟用 Seq。</summary>
    public bool Enabled { get; set; } = false;

    /// <summary>Seq 伺服器網址。</summary>
    public string ServerUrl { get; set; } = string.Empty;

    /// <summary>本地緩衝相對路徑。</summary>
    public string BufferRelativePath { get; set; } = "seq-buffer";

    /// <summary>傳送週期秒數。</summary>
    public int PeriodSeconds { get; set; } = 10;
}
