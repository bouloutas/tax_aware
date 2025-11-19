#!/usr/bin/env python3
"""
Run the FastAPI server.

Usage:
    python scripts/run_api.py
"""
import uvicorn
from src.core.config import Config

if __name__ == "__main__":
    uvicorn.run(
        "src.api.main:app",
        host=Config.API_HOST,
        port=Config.API_PORT,
        reload=Config.API_RELOAD,
    )

