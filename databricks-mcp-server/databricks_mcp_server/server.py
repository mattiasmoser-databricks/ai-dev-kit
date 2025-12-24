"""
Databricks MCP Server

FastAPI server implementing MCP protocol over HTTP (for remote access).
For local development, use stdio transport via: python -m databricks_mcp_server.stdio_server
"""
from fastapi import FastAPI, Request

from .message_handler import handle_mcp_message

app = FastAPI(title="Databricks MCP Server (HTTP)")


@app.post("/message")
async def message_endpoint(request: Request):
    """Handle MCP JSON-RPC messages"""
    request_data = await request.json()
    response = handle_mcp_message(request_data)
    return response


@app.get("/")
async def health():
    """Health check endpoint"""
    return {"status": "ok", "transport": "http"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
