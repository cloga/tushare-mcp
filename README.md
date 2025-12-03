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

## Using with GitHub Copilot in VS Code

To use this MCP server with GitHub Copilot in VS Code, you need to configure the `mcp.json` file.

1.  **Open Configuration**:
    *   Open the Command Palette (`Ctrl+Shift+P` or `F1`).
    *   Search for and select **`MCP: Configure MCP Servers`**.
    *   This will open the `mcp.json` file (typically located in `%APPDATA%\Code\User\mcp.json` on Windows).

2.  **Add Server Configuration**:
    Add the `tushare-server` configuration to the JSON file. Make sure to use absolute paths for both the Python executable and the script.

    ```json
    {
      "mcpServers": {
        "tushare-server": {
          "command": "C:\\path\\to\\your\\python.exe",
          "args": [
            "C:\\path\\to\\tushare_mcp_server\\server.py"
          ]
        }
      }
    }
    ```

    *   Replace `C:\\path\\to\\your\\python.exe` with your actual Python interpreter path (e.g., `C:\\Users\\username\\AppData\\Local\\Programs\\Python\\Python311\\python.exe`).
    *   Replace `C:\\path\\to\\tushare_mcp_server\\server.py` with the absolute path to this project's `server.py`.

3.  **Restart VS Code**:
    After saving `mcp.json`, restart VS Code for the changes to take effect.

## Usage

### Testing with MCP Inspector

You can test the server using the MCP Inspector:

```bash
npx @modelcontextprotocol/inspector python server.py
```

### Using with Claude Desktop

Add the following configuration to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "tushare": {
      "command": "python",
      "args": ["C:\\Users\\lochen\\tushare_mcp_server\\server.py"],
      "env": {
        "TUSHARE_TOKEN": "your_token_here"
      }
    }
  }
}
```

Make sure to replace `your_token_here` with your actual Tushare token.
