# 資料歸檔排程系統說明

本專案實作以 SQL Server + C# 背景常駐服務（WinExe）為基礎的資料歸檔機制，採用 Dapper 進行資料存取並搭配 Serilog 記錄執行過程。

## 執行流程
1. **Host 啟動**：`Program` 透過 Serilog 引導式 logger 啟動，載入 `appsettings.json` 的連線與歸檔設定，註冊 `Worker` 常駐服務。
2. **週期排程**：`Worker` 在啟動時先執行一次歸檔流程，之後每 24 小時再執行一次。任何例外都會寫入日誌並於下一次週期再試。
3. **搬移線上庫 → 歷史庫**：`ArchiveCoordinator` 逐表讀取設定，使用 `TransactionScope` 在同一批次中先插入 DB2 再刪除 DB1，確保不會只搬一半資料。搬移時以日期欄位判斷超過保留期的資料並依批次大小處理。
4. **歷史庫 → CSV**：當資料在歷史庫中也超過保留期時，批次抓取後依檔案上限分段輸出 CSV，並根據表名與年月建立資料夾；輸出成功後刪除已匯出的資料。
5. **重試策略**：每個搬移或匯出批次都套用簡易重試策略（次數與延遲可配置），避免短暫錯誤阻塞整體流程。

## 設定重點
- `appsettings.json` 透過 `ArchiveDefaults` 給出預設批次大小、保留月數、CSV 參數與重試策略，表級設定改由 DB 管理。
- `ConnectionStrings` 指定 DB1/DB2 連線；`Logging` 控制 Serilog 最小等級、檔案/Seq sink 行為。

## 模組化設計
- `SqlConnectionFactory`：統一依連線名稱產生 `SqlConnection`。
- `RetryPolicyExecutor`：包裝可配置的重試邏輯。
- `ArchiveCoordinator`：核心協調器，負責搬移與匯出流程、CSV 寫檔與刪除。
- `DynamicSqlHelper`：根據動態資料列產生 Insert/Delete SQL，避免硬編碼欄位。

## 可靠性與防呆
- 每批次操作使用交易範圍確保「插入成功才刪除」。
- Insert 語句內含 `NOT EXISTS`，以主鍵避免重複搬移。
- CSV 輸出前先建立資料夾並寫入表頭；內容值會做分隔符號跳脫。
- 以批次分段讀寫避免鎖表與記憶體壓力。

## 延伸建議
- 若環境允許 MSDTC，可將 `TransactionScope` 調整為特定隔離層級。
- 可在 `Tables` 中加入更多欄位過濾或排序需求，以適配不同場景。

## 後續可視化設定重構

若需要改為「後台 UI 可動態調整歸檔設定」的方案，請參考 `Documentation/ArchiveSystemRedesign.md`，內含：

- Solution 拆分（Worker / Web Admin / Domain / Application / Infrastructure）
- 設定落地策略（哪些留在 appsettings、哪些存 DB）、完整 SQL schema
- MVC 後台的 ViewModel、Controller 範例與驗證規則
- IArchiveSettingsProvider 介面與 Dapper 實作（DbArchiveSettingsProvider）、Worker 冪等批次搬移流程
- 架構圖、流程圖、類別關聯、效能與風險分析
