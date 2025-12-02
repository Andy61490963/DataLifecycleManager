using DataLifecycleManager.Configuration;
using DataLifecycleManager.Infrastructure;
using DataLifecycleManager.Logging;
using DataLifecycleManager.Repositories;
using DataLifecycleManager.Services;
using Serilog;

namespace DataLifecycleManager;

/// <summary>
/// MVC 應用程式入口，負責建置 WebApplication 並註冊 MVC 相關服務。
/// </summary>
public class Program
{
    /// <summary>
    /// 建置並啟動 ASP.NET Core MVC 應用程式。
    /// </summary>
    /// <param name="args">啟動參數。</param>
    public static void Main(string[] args)
    {
        Log.Logger = SerilogConfigurator.CreateBootstrapLogger();

        var builder = WebApplication.CreateBuilder(args);

        builder.Host.UseSerilog((context, services, loggerConfiguration) =>
        {
            var loggingOptions = context.Configuration
                .GetSection("AppLogging")
                .Get<AppLoggingOptions>() ?? new AppLoggingOptions();

            SerilogConfigurator.Configure(loggerConfiguration, loggingOptions);
        });

        builder.Services.Configure<AppLoggingOptions>(builder.Configuration.GetSection("AppLogging"));
        builder.Services.Configure<RetryPolicySettings>(builder.Configuration.GetSection("RetryPolicy"));
        builder.Services.Configure<CsvOptions>(builder.Configuration.GetSection("CsvOptions"));

        builder.Services.AddSingleton<SqlConnectionFactory>();
        builder.Services.AddSingleton<RetryPolicyExecutor>();
        builder.Services.AddScoped<IArchiveSettingRepository, ArchiveSettingRepository>();
        builder.Services.AddScoped<ArchiveExecutionService>();

        builder.Services.AddControllersWithViews();

        var app = builder.Build();

        if (app.Environment.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }
        else
        {
            app.UseExceptionHandler("/Home/Error");
            app.UseHsts();
        }

        app.UseHttpsRedirection();
        app.UseStaticFiles();

        app.UseRouting();
        app.UseAuthorization();

        app.MapControllerRoute(
            name: "default",
            pattern: "{controller=ArchiveSettings}/{action=Index}/{id?}");

        app.Run();
    }
}
