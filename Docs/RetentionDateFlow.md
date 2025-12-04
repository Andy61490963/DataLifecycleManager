# 保留日期設定與搬移流程說明

本文說明線上 / 歷史資料「保留截止日期」的運作方式以及前後端邏輯，協助維護與調校設定。

## 前端（Views/ArchiveSettings/Index.cshtml）
- 表單以 Bootstrap Input Group 呈現兩個日期欄位：
  - **線上資料保留截止日期** (`Form.OnlineRetentionDate`)
  - **歷史資料保留截止日期** (`Form.HistoryRetentionDate`)
- JavaScript 會在 `DOMContentLoaded` 後呼叫 `setupRetentionGuards()`，於兩日期欄位的 `change` 事件中檢查：
  - 若線上截止日小於或等於歷史截止日，兩欄位皆套用 `setCustomValidity` 提示，提交時會被阻擋。
- Validation Summary 及欄位錯誤訊息同樣會顯示來自伺服器端的驗證結果，確保前後端一致。

## 後端 ViewModel（ViewModels/ArchiveSettingInputModel.cs）
- 兩個日期屬性使用 `[DataType(DataType.Date)]` 方便 MVC Model Binding 與顯示格式。
- 預設值分別為「今日往前 3 個月」與「今日往前 6 個月」，方便初次輸入。
- 實作 `IValidatableObject.Validate`，若線上截止日未晚於歷史截止日即回傳 `ValidationResult`，防止錯誤設定寫入資料庫。

## 控制器（Controllers/ArchiveSettingsController.cs）
- `Save` Action 接收 `ArchiveSettingsPageViewModel`，當 `ModelState.IsValid` 為 false 時會重新帶回原列表與驗證訊息。
- 成功時會將日期以 `.Date` 存入 `ArchiveSetting`，避免時間成分造成比較誤差。

## 資料存取層（Repositories/ArchiveSettingRepository.cs）
- Dapper 查詢、插入、更新均改用 `OnlineRetentionDate` 與 `HistoryRetentionDate` 欄位。
- 依舊透過 `SqlConnectionFactory` 以 `ConfigurationDb` 連線名稱取得設定資料庫連線。

## 搬移服務（Services/ArchiveExecutionService.cs）
- 每筆啟用的設定在執行前會先比較線上 / 歷史截止日，若日期順序不符則記錄警告並跳過。
- 搬移線上庫 → 歷史庫時，以 `OnlineRetentionDate` 為查詢門檻；匯出 CSV 時以 `HistoryRetentionDate` 為門檻。
- 其餘批次處理、CSV 匯出與刪除流程不變。

## 可能的問題與防範
- **日期未帶入時的 Null**：表單採必填且預設值提供，伺服器端仍有驗證防堵無效日期。
- **時區差異**：儲存前使用 `.Date` 確保只比對日期，避免時區或時間部分造成誤判。
- **既有資料轉換**：資料表需提前新增 `OnlineRetentionDate`、`HistoryRetentionDate` 欄位並遷移舊資料，可依原月份邏輯批次計算日期填入。
