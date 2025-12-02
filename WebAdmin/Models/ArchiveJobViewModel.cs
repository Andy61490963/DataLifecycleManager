using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;

namespace DataLifecycleManager.WebAdmin.Models;

/// <summary>
/// 後台設定畫面使用的歸檔任務 ViewModel。
/// </summary>
public class ArchiveJobViewModel
{
    [HiddenInput]
    public int? Id { get; set; }

    [Required, StringLength(128)]
    public string JobName { get; set; } = string.Empty;

    [Required, StringLength(256)]
    public string TableName { get; set; } = string.Empty;

    [Required, StringLength(128)]
    public string SourceConnection { get; set; } = string.Empty;

    [Required, StringLength(128)]
    public string TargetConnection { get; set; } = string.Empty;

    [Required, StringLength(128)]
    public string DateColumn { get; set; } = string.Empty;

    [Required, StringLength(128)]
    public string PrimaryKeyColumn { get; set; } = string.Empty;

    [Range(1, 120)]
    public int OnlineRetentionMonths { get; set; } = 3;

    [Range(0, 120)]
    public int HistoryRetentionMonths { get; set; } = 6;

    [Range(1, int.MaxValue)]
    public int BatchSize { get; set; } = 1000;

    public bool CsvEnabled { get; set; } = true;

    [StringLength(400)]
    public string? CsvRootFolder { get; set; }

    public bool Enabled { get; set; } = true;

    [Range(0, 10)]
    public int? RetryCount { get; set; }

    [Range(0, 300)]
    public int? RetryDelaySeconds { get; set; }
}
