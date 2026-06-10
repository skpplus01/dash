# 永豐 Shioaji 微型臺指期貨即時報價儀表板

這個專案提供一個可在 **Google Colab / Jupyter Notebook / 本機 Python** 執行的永豐金 Shioaji 即時報價儀表板，用來訂閱並顯示微型臺指期貨（Micro TAIEX Futures，商品代碼 `TMF`）的 Tick / BidAsk / Quote 報價。

> ⚠️ 僅供行情顯示與程式範例使用，不含下單功能，也不構成投資建議。永豐即時報價只會在期交所交易時段推送。

## 需要登入 API 嗎？

需要。Shioaji 目前以 **API Key + Secret Key** 登入；你有 API Key 之後，還需要建立時取得的 Secret Key 才能完成 `api.login(...)`。這個專案不會要求券商帳號密碼，也不會啟用憑證或下單功能。

在 Colab 建議用隱藏輸入，不要把 KEY 寫死在 Notebook：

```python
from micro_taiex_dashboard import run_colab_dashboard

dashboard = run_colab_dashboard(
    contract_code="TMFR1",
    quote_types=("tick", "bid_ask"),
    simulation=True,
)
```

執行後會依序詢問 `永豐 API Key` 與 `永豐 Secret Key`。如果你想手動傳入，也可以使用 `run_colab_dashboard(api_key="...", secret_key="...")`。

## 功能

- 使用永豐 Shioaji API 登入並訂閱期貨即時報價。
- 預設訂閱微型臺指近月連續契約 `TMFR1`，也可改成指定月份契約，例如 `TMF202606`。
- 支援 `tick`、`bid_ask`、`quote` 三種報價類型。
- 在 Colab / Jupyter 以 `ipywidgets` 顯示即時儀表板：
  - 最新成交價、成交量、時間、漲跌方向。
  - 即時價格變化面板：若還沒有新成交 Tick，會用 BidAsk 的買賣中價當參考價，讓你看到盤中買賣價變動。
  - 最佳買賣價量（若訂閱 BidAsk 或回傳資料包含該欄位）。
  - 最近 N 筆報價表格。
  - Tick 與 BidAsk 交錯推送時，畫面會保留最新成交價，並持續更新買賣價量；不會因最新一筆是 BidAsk 而把成交價顯示成 `--`。
- 可用 CLI 在終端機列印即時報價，方便先確認帳號與契約代碼。

## 快速開始：Google Colab

1. 開啟 `notebooks/colab_micro_taiex_dashboard.ipynb`，或在 Colab 建立新 Notebook。
2. 安裝套件：

```python
!pip install -q shioaji ipywidgets nest-asyncio
```

3. 下載或上傳本專案的 `micro_taiex_dashboard.py`，然後執行：

```python
from micro_taiex_dashboard import run_colab_dashboard

# 會用隱藏輸入詢問永豐 API Key 與 Secret Key；不要把 KEY 寫死在 Notebook。
dashboard = run_colab_dashboard(
    contract_code="TMFR1",   # 微型臺指近月連續契約；也可填 TMF202606 等月份契約
    quote_types=("tick", "bid_ask"),
    simulation=True,
)
```

4. 不再接收行情時：

```python
dashboard.stop()
```

## 本機 / 終端機執行

```bash
export SINOPAC_API_KEY="你的 API key"      # 或使用 SJ_API_KEY
export SINOPAC_SECRET_KEY="你的 Secret key"  # 或使用 SJ_SEC_KEY
python micro_taiex_dashboard.py --contract TMFR1 --quote-type tick --quote-type bid_ask
```

## 如果畫面停在「等待第一筆報價」

- 確認目前是期交所微型臺指期貨交易時段；非交易時段訂閱會成功，但不會立刻有新 Tick / BidAsk。
- 若你想看到連續跳動，請保留預設 `quote_types=("tick", "bid_ask")`；成交不頻繁時，BidAsk 的買賣中價仍會在「即時價格變化」面板更新。
- 確認儀表板狀態列顯示的契約代碼正確，例如 `TMFF6`；若仍顯示 `TMFR1` 且沒有資料，請改用實際月份契約。
- 若狀態列顯示「訂閱失敗」，請依錯誤訊息檢查 API 權限、契約代碼或登入狀態。
- 若重新執行 Colab cell，建議先執行 `dashboard.stop()` 再重新啟動，避免舊 callback 仍掛在前一次的 dashboard 物件上。

## 契約代碼說明

- 微型臺指期貨的英文商品代碼是 `TMF`。
- `TMFR1`：近月連續契約；啟動儀表板時會優先解析成目前實際交易月份契約來訂閱，例如 `TMFF6`。
- `TMFR2`：次近月連續契約。
- `TMFYYYYMM`：指定月份契約，例如 `TMF202606`。

若永豐 API 的合約樹尚未提供 `TMFR1`，請在登入後列出可用契約，並改用實際月份契約：

```python
from micro_taiex_dashboard import login_api, list_product_contracts
api = login_api(api_key, secret_key)
list_product_contracts(api, "TMF")
```

## 重要注意事項

- Colab 可能因瀏覽器分頁休眠、執行階段中斷或網路重連而停止收報價。
- 即時報價必須在市場開盤且帳號權限允許的情況下才會收到資料。
- 本範例不啟用憑證、不下單；若你自行加入交易功能，請務必先了解風險並使用模擬環境測試。
