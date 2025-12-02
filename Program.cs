using DataLifecycleManager.Configuration;
using DataLifecycleManager.Configuration;
using DataLifecycleManager.Domain;
using DataLifecycleManager.Infrastructure;
using DataLifecycleManager.Infrastructure.Providers;
using DataLifecycleManager.Repositories;
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
        // 先建一個 bootstrap logger，讓 Program 啟動階段也有 log
        Log.Logger = SerilogConfigurator.CreateBootstrapLogger();

        try
        {
            CreateHostBuilder(args).Build().Run();
        }
        catch (Exception ex)
        {
            // 啟動階段或 Host.Run 期間如果炸掉，這邊會記錄
            Log.Fatal(ex, "Application terminated unexpectedly");
        }
        finally
        {
            // 確保 buffer 裡的 log 都寫完
            Log.CloseAndFlush();
        }
    }

    /// <summary>
    /// 建立 IHostBuilder 並註冊服務與 Serilog。
    /// </summary>
    /// <param name="args">啟動參數。</param>
    /// <returns>IHostBuilder 實例。</returns>
    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            // 這裡就可以用 UseSerilog，因為現在是 IHostBuilder
            .UseSerilog((context, services, loggerConfiguration) =>
            {
                var loggingOptions = context.Configuration
                    .GetSection("AppLogging")
                    .Get<AppLoggingOptions>() ?? new AppLoggingOptions();

                SerilogConfigurator.Configure(loggerConfiguration, loggingOptions);
            })
            .ConfigureServices((context, services) =>
            {
                services.Configure<ArchiveDefaultsOptions>(
                    context.Configuration.GetSection("ArchiveDefaults"));

                services.Configure<AppLoggingOptions>(
                    context.Configuration.GetSection("AppLogging"));

                services.AddSingleton<SqlConnectionFactory>();
                services.AddSingleton<IArchiveJobRepository, ArchiveJobRepository>();
                services.AddSingleton<IArchiveSettingsProvider, DbArchiveSettingsProvider>();
                services.AddSingleton<RetryPolicyExecutor>();
                services.AddSingleton<ArchiveCoordinator>();
                services.AddHostedService<Worker>();
            });
}
