using System.Text;
using DataLifecycleManager.Configuration;
using Serilog;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;

namespace DataLifecycleManager.Logging;

/// <summary>
/// 負責初始化與設定 Serilog，支援啟動前後的兩階段設定模式。
/// </summary>
internal static class SerilogConfigurator
{
    private const string LogDirectoryName = "Logs";
    private const string LogFilePattern = "app-.log";

    /// <summary>
    /// 建立啟動初期使用的 Logger，僅輸出 Console，以確保穩定啟動。
    /// </summary>
    /// <returns>初始 Logger。</returns>
    public static Logger CreateBootstrapLogger()
    {
        return new LoggerConfiguration()
            .MinimumLevel.Information()
            .WriteTo.Console()
            .CreateLogger();
    }

    /// <summary>
    /// 依照設定檔建置正式的 Logger 管線，包含檔案與 Seq Sink。
    /// </summary>
    /// <param name="loggerConfiguration">Serilog 設定器。</param>
    /// <param name="options">外部傳入的紀錄設定。</param>
    public static void Configure(LoggerConfiguration loggerConfiguration, AppLoggingOptions options)
    {
        ArgumentNullException.ThrowIfNull(loggerConfiguration);
        ArgumentNullException.ThrowIfNull(options);

        var logDirectory = ResolveLogDirectory();
        SetupSelfLog(logDirectory);

        SetMinimumLevel(loggerConfiguration, options.MinimumLevel);

        loggerConfiguration
            .Enrich.FromLogContext()
            .Enrich.WithProperty("Application", options.ApplicationName);

        if (options.File.Enabled)
        {
            loggerConfiguration.WriteTo.File(
                path: Path.Combine(logDirectory, LogFilePattern),
                rollingInterval: RollingInterval.Day,
                retainedFileCountLimit: options.File.RetainDays,
                encoding: Encoding.UTF8,
                fileSizeLimitBytes: options.File.FileSizeLimitMB * 1024L * 1024L,
                rollOnFileSizeLimit: true,
                shared: true
            );
        }

        if (options.Seq.Enabled && !string.IsNullOrWhiteSpace(options.Seq.ServerUrl))
        {
            loggerConfiguration.WriteTo.Seq(
                serverUrl: options.Seq.ServerUrl,
                bufferBaseFilename: Path.Combine(logDirectory, options.Seq.BufferRelativePath),
                period: TimeSpan.FromSeconds(options.Seq.PeriodSeconds)
            );
        }
    }

    private static string ResolveLogDirectory()
    {
        try
        {
            var projectRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", ".."));
            if (Directory.Exists(Path.Combine(projectRoot, "bin")))
            {
                var dev = Path.Combine(projectRoot, LogDirectoryName);
                Directory.CreateDirectory(dev);
                return dev;
            }

            var normal = Path.Combine(AppContext.BaseDirectory, LogDirectoryName);
            Directory.CreateDirectory(normal);
            return normal;
        }
        catch (Exception ex) when (ex is UnauthorizedAccessException or IOException)
        {
            var fallback = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "DataLifecycleManager", LogDirectoryName);
            Directory.CreateDirectory(fallback);
            SelfLog.WriteLine("日誌目錄改用備援路徑 {0}：{1}", fallback, ex.Message);
            return fallback;
        }
    }

    private static void SetupSelfLog(string logDirectory)
    {
        var selfLogPath = Path.Combine(logDirectory, "serilog-selflog.txt");

        SelfLog.Enable(msg =>
        {
            try
            {
                File.AppendAllText(selfLogPath, msg);
            }
            catch
            {
            }
        });
    }

    private static void SetMinimumLevel(LoggerConfiguration config, string level)
    {
        var normalized = (level ?? string.Empty).Trim().ToLowerInvariant();

        config.MinimumLevel.Is(normalized switch
        {
            "verbose" => LogEventLevel.Verbose,
            "debug" => LogEventLevel.Debug,
            "warning" => LogEventLevel.Warning,
            "error" => LogEventLevel.Error,
            "fatal" => LogEventLevel.Fatal,
            _ => LogEventLevel.Information
        });
    }
}
