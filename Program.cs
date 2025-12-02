using DataLifecycleManager.Configuration;
using DataLifecycleManager.Infrastructure;
using DataLifecycleManager.Logging;
using DataLifecycleManager.Services;
using Serilog;

namespace DataLifecycleManager;

/// <summary>
/// 主程式入口，建立 Host 並註冊排程服務。
/// </summary>
public class Program
{
    /// <summary>
    /// 建置並啟動背景常駐服務。
    /// </summary>
    /// <param name="args">啟動參數。</param>
    public static void Main(string[] args)
    {
        Log.Logger = SerilogConfigurator.CreateBootstrapLogger();

        var builder = Host.CreateApplicationBuilder(args);
        builder.Services.Configure<ArchiveSettings>(builder.Configuration.GetSection("ArchiveSettings"));
        builder.Services.Configure<AppLoggingOptions>(builder.Configuration.GetSection("Logging"));
        builder.Services.AddSingleton<SqlConnectionFactory>();
        builder.Services.AddSingleton<RetryPolicyExecutor>();
        builder.Services.AddSingleton<ArchiveCoordinator>();
        builder.Services.AddHostedService<Worker>();

        builder.Host.UseSerilog((context, services, loggerConfiguration) =>
        {
            var loggingOptions = context.Configuration.GetSection("Logging").Get<AppLoggingOptions>() ?? new AppLoggingOptions();
            SerilogConfigurator.Configure(loggerConfiguration, loggingOptions);
        });

        var host = builder.Build();
        host.Run();
    }
}
