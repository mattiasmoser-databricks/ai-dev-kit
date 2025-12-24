# Databricks MCP Server

MCP (Model Context Protocol) server that exposes Databricks operations as AI-friendly tools.

## Overview

This is a thin wrapper around `databricks-mcp-core` that implements the MCP protocol using:
- **stdio transport** (recommended for local development) - uses standard input/output
- **HTTP transport** (for remote access) - FastAPI server with JSON-RPC endpoints

## Available Tools

### Compute Operations (4 tools)
- `create_context` - Create execution context on cluster
- `execute_command_with_context` - Execute code with state persistence
- `destroy_context` - Clean up execution context
- `databricks_command` - One-off command execution

### Unity Catalog Operations (11 tools)
- **Catalogs:** `list_catalogs`, `get_catalog`
- **Schemas:** `list_schemas`, `get_schema`, `create_schema`, `update_schema`, `delete_schema`
- **Tables:** `list_tables`, `get_table`, `create_table`, `delete_table`

## Installation

```bash
# Install both core and server packages
pip install -e ../databricks-mcp-core
pip install -e .
```

## Configuration

Three authentication methods (in priority order):

### 1. Databricks CLI Profile (Recommended)

Use your existing `~/.databrickscfg` profile:

```bash
export DATABRICKS_CONFIG_PROFILE=ai-strat
```

Or in your `.env` file:

```
DATABRICKS_CONFIG_PROFILE=ai-strat
```

### 2. Environment Variables

```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
```

### 3. .env File

```
DATABRICKS_HOST=https://your-workspace.databricks.com
DATABRICKS_TOKEN=dapi...
```

## Running the Server

### Local Development (stdio - Recommended)

The stdio transport is recommended for local development. It's automatically used when configured in Claude/Cursor MCP settings.

**Manual testing:**
```bash
python -m databricks_mcp_server.stdio_server
```

### Remote Access (HTTP)

For remote access or when HTTP is preferred:

```bash
# Development mode
uvicorn databricks_mcp_server.server:app --host 127.0.0.1 --port 8000

# Production mode
uvicorn databricks_mcp_server.server:app --host 0.0.0.0 --port 8000
```

**HTTP Endpoints:**
- `POST /message` - JSON-RPC message handling
- `GET /` - Health check

## Usage with Claude Code

### Recommended: stdio Transport (Local)

Add to `.claude/.mcp.json`:

```json
{
  "mcpServers": {
    "ai-dev-kit": {
      "command": "uv",
      "args": ["run", "python", "-m", "databricks_mcp_server.stdio_server"],
      "transport": "stdio"
    }
  }
}
```

Or using pip:

```json
{
  "mcpServers": {
    "ai-dev-kit": {
      "command": "python",
      "args": ["-m", "databricks_mcp_server.stdio_server"],
      "transport": "stdio"
    }
  }
}
```

### Alternative: HTTP Transport (Remote)

```json
{
  "mcpServers": {
    "ai-dev-kit": {
      "url": "http://localhost:8000/message",
      "transport": "http-stream"
    }
  }
}
```

## Architecture

```
databricks-mcp-server/
├── stdio_server.py        # stdio transport (recommended for local)
├── server.py              # HTTP transport (for remote access)
├── message_handler.py     # Shared MCP message handling logic
└── tools/
    ├── unity_catalog.py   # UC tool wrappers
    ├── compute.py         # Compute tool wrappers
    ├── spark_declarative_pipelines.py  # Pipeline tool wrappers
    └── synthetic_data_generation.py     # Synthetic data tool wrappers
```

Each tool wrapper:
1. Imports functions from `databricks-mcp-core`
2. Wraps them with MCP response formatting
3. Provides JSON schema definitions
