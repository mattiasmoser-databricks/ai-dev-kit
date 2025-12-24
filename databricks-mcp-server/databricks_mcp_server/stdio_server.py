"""
MCP Server using stdio transport (recommended for local development).

Reads JSON-RPC messages from stdin and writes responses to stdout.
Uses line-delimited JSON (NDJSON) format.
"""
import json
import sys
from typing import Dict, Any

from .message_handler import handle_mcp_message


def run_stdio_server():
    """
    Run MCP server using stdio transport.
    
    Reads line-delimited JSON from stdin and writes responses to stdout.
    """
    # Use unbuffered I/O for real-time communication
    sys.stdin.reconfigure(encoding='utf-8', errors='strict')
    sys.stdout.reconfigure(encoding='utf-8', errors='strict', line_buffering=True)
    sys.stderr.reconfigure(encoding='utf-8', errors='strict', line_buffering=True)
    
    try:
        # Read messages from stdin (line-delimited JSON)
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
                
            try:
                # Parse JSON-RPC request
                request_data = json.loads(line)
                
                # Handle message and get response
                response = handle_mcp_message(request_data)
                
                # Write response to stdout (line-delimited JSON)
                response_line = json.dumps(response, ensure_ascii=False)
                sys.stdout.write(response_line + '\n')
                sys.stdout.flush()
                
            except json.JSONDecodeError as e:
                # Send error response for invalid JSON
                error_response = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": -32700,
                        "message": f"Parse error: {str(e)}"
                    }
                }
                sys.stdout.write(json.dumps(error_response) + '\n')
                sys.stdout.flush()
                
    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C
        pass
    except Exception as e:
        # Send error response for unexpected errors
        error_response = {
            "jsonrpc": "2.0",
            "id": None,
            "error": {
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            }
        }
        sys.stdout.write(json.dumps(error_response) + '\n')
        sys.stdout.flush()
        sys.exit(1)


if __name__ == "__main__":
    run_stdio_server()

