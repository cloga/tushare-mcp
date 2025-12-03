# Tushare MCP Server

This is a Model Context Protocol (MCP) server that provides access to Tushare financial data.

## Prerequisites

- Python 3.10 or higher
- A Tushare Token (Get it from [Tushare.pro](https://tushare.pro/))

## Installation

1.  Install the dependencies:
  ```bash
  pip install -r requirements.txt
  ```

## Configuration

1. Create a `.env` file in the project root directory:
   ```env
   TUSHARE_TOKEN=your_tushare_token_here
   ```

## Project Layout

- `server/`: MCP entrypoint (`server.py`) plus logs.
- `src/`: application code split into `strategies/` (wheel backtest, etc.) and `utils/` (shared math + performance helpers).
- `temp_data/`: generated artifacts such as `wheel_report.html`, `wheel_dashboard.html`, and portfolio CSV/JSON outputs.
- `tests/`: standalone scripts for strategy prototyping (`wheel_strategy.py`, `portfolio_rebalance.py`, etc.).

## Using with GitHub Copilot in VS Code

To use this MCP server with GitHub Copilot in VS Code, you need to configure the `mcp.json` file.

1.  **Open Configuration**:
    *   Open the Command Palette (`Ctrl+Shift+P` or `F1`).
    *   Search for and select **`MCP: Configure MCP Servers`**.
    *   This will open the `mcp.json` file (typically located in `%APPDATA%\Code\User\mcp.json` on Windows).

2.  **Add via Command Palette (quick way)**:
  *   Press `Ctrl+Shift+P` again and pick **`MCP: Add MCP Server`**.
  *   When the **Enter Command** prompt appears, paste:
    ```
    C:\Users\lochen\AppData\Local\Microsoft\WindowsApps\python3.13.exe c:\Users\lochen\tushare-mcp\server\server.py
    ```
  *   Accept the suggested server name (for example `tushare`) and save.

3.  **Add Server Configuration manually**:
    Add the `tushare-server` configuration to the JSON file. Make sure to use absolute paths for both the Python executable and the script.

    ```json
    {
      "mcpServers": {
        "tushare-server": {
          "command": "C:\\path\\to\\your\\python.exe",
          "args": [
            "C:\\path\\to\\tushare_mcp_server\\server\\server.py"
          ]
        }
      }
    }
    ```

    *   Replace `C:\\path\\to\\your\\python.exe` with your actual Python interpreter path (e.g., `C:\\Users\\username\\AppData\\Local\\Programs\\Python\\Python311\\python.exe`).
    *   Replace `C:\\path\\to\\tushare_mcp_server\\server\\server.py` with the absolute path to this project's server entry point.

3.  **Restart VS Code**:
    After saving `mcp.json`, restart VS Code for the changes to take effect.

## Usage

### Testing with MCP Inspector

You can test the server using the MCP Inspector:

```bash
npx @modelcontextprotocol/inspector python server/server.py
```

### Price Volatility Tool

After the server is running (Inspector, Copilot, etc.), invoke the `get_price_volatility` tool to compute recent volatility for a stock. It supports `frequency` values `daily`, `monthly`, or `yearly`, letting you measure 波动率 on日线/月线/年线 data.

```json
{
  "tool": "get_price_volatility",
  "args": {
    "identifier": "000001.SZ",
    "window": 30,
    "frequency": "daily",
    "annualize": true
  }
}
```

Key fields returned:
- `frequency`: 数据频率（daily/monthly/yearly）。
- `window_periods`: 实际使用的周期数（交易日/月份/年份）。
- `period_volatility`: 该频率下的标准差。
- `annualized_volatility`: 根据频率自动换算后的年化波动率（252/12/1）。
- `mean_period_return`: 平均单周期收益。

### Option Reference & Daily Data

基于 [Tushare 文档 #157](https://tushare.pro/document/2?doc_id=157) 新增了两个期权工具：

- `get_option_basic(exchange, fields)`: 返回上/深交所上市期权的合约元数据（行权价、类型、到期日等）。
- `get_option_daily(ts_code, trade_date, start_date, end_date, exchange)`: 返回期权日线行情。

示例（通过 MCP 调用 `get_option_daily` 查询科创50期权在单日的 K 线）：

```json
{
  "tool": "get_option_daily",
  "args": {
    "exchange": "SSE",
    "trade_date": "20251203"
  }
}
```

返回值为 JSON 数组，字段与 Tushare `opt_daily` 接口一致，可配合 `get_price_volatility` 等工具进一步分析期权策略。

### ETF/Fund Daily Quotes

使用新的 `get_fund_daily` 工具（Tushare 文档 #127 对应接口）可直接拉取 ETF/场内基金的日线行情：

```json
{
  "tool": "get_fund_daily",
  "args": {
    "ts_code": "159915.SZ",
    "start_date": "20250101",
    "end_date": "20251203",
    "fields": "ts_code,trade_date,open,high,low,close,vol"
  }
}
```

当 `fields` 为空时会返回默认全部列。可搭配 `get_option_daily`、`get_price_volatility` 等工具构建 ETF+期权策略分析。

### Wheel Strategy Backtest

`tests/wheel_strategy.py` 利用 `159915.SZ`（创业板ETF）及其期权，ETF 行情通过 Tushare `fund_daily` 接口（[文档 #127](https://tushare.pro/document/2?doc_id=127)）获取，模拟“车轮饼”策略：

1. 每个自然月首个交易日：
  - 若空仓，卖出 5%~10% OTM 的认沽合约（`call_put='P'`）。
  - 若持仓，卖出 5%~10% OTM 的认购合约（`call_put='C'`）。
2. 到期日根据标的收盘价判断是否被指派，按轮动逻辑更新持仓。
3. 统计权利金、被指派次数、最大保证金占用，并估算收益率。
4. 报告会显示每笔交易的隐含波动率：若 Tushare 返回该字段则直接使用，否则基于 Black-Scholes（假定 2% 无风险利率）用成交价反推出一个参考值。

运行：

```bash
python tests/wheel_strategy.py
```

输出包含总收益、占用保证金、近10期交易记录等。所有生成的 `wheel_report.html`、`wheel_dashboard.html` 等文件都会写入 `temp_data/`，便于统一清理。若需调整标的、时间区间或虚值区间，可编辑脚本顶部的常量（`UNDERLYING`、`START_DATE`、`OTM_RANGE` 等）。

### Wheel Strategy MCP Tool

无需运行脚本，也可直接通过 MCP 调用 `backtest_wheel_strategy`：

```json
{
  "tool": "backtest_wheel_strategy",
  "args": {
    "underlying": "159915.SZ",
    "start_date": "20230101",
    "end_date": "20251203",
    "otm_min": 0.05,
    "otm_max": 0.10,
    "initial_capital": 30000
  }
}
```

返回 JSON 中包含：
- `ending_value`、`return_on_capital`、`annualized_return` 等指标。
- `recent_trades`：近 12 期的期权选择、权利金、行权价及是否被指派。

可通过参数更换标的（只要该 ETF 有挂牌期权）、调整虚值区间或回测时间窗。确保 Tushare Token 对 `fund_daily`、`opt_basic`、`opt_daily` 接口有权限。

### Multi-ETF Portfolio Backtest

`tests/portfolio_rebalance.py` 按照截图中的 10 只 ETF 及固定权重构建组合，并在每个自然月首个交易日动态再平衡：

```bash
python tests/portfolio_rebalance.py
```

脚本会：
- 自动获取所有 ETF 的可用历史区间，并截取重叠部分；
- 计算每日组合净值并输出 `temp_data/portfolio_equity_curve.csv`，再平衡明细写入 `temp_data/portfolio_rebalances.csv`；
- 生成 `temp_data/portfolio_vs_benchmarks.csv`，其中包含组合与沪深300/中证500/创业板指的归一化指数曲线；
- 在 `temp_data/portfolio_summary.json` 中汇总收益率、年化波动率、最大回撤、Sharpe Ratio 及各基准指数的对比指标。

### Using with Claude Desktop

Add the following configuration to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "tushare": {
      "command": "python",
      "args": ["C:\\Users\\lochen\\tushare_mcp_server\\server\\server.py"],
      "env": {
        "TUSHARE_TOKEN": "your_token_here"
      }
    }
  }
}
```

Make sure to replace `your_token_here` with your actual Tushare token.
