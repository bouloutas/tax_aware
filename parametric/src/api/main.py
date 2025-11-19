"""
Main FastAPI application.

Provides REST API endpoints for the tax-aware portfolio management system.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import accounts, optimization, rebalancing, reporting, tax_harvesting
from src.core.config import Config
from src.core.database import create_database_engine, get_session_factory


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI app."""
    # Startup
    engine = create_database_engine(Config.get_database_url(), echo=False)
    app.state.engine = engine
    app.state.Session = get_session_factory(engine)
    yield
    # Shutdown
    app.state.engine.dispose()


app = FastAPI(
    title="Tax-Aware Portfolio Management API",
    description="REST API for tax-aware portfolio management with automated tax-loss harvesting",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(accounts.router, prefix="/api/accounts", tags=["accounts"])
app.include_router(tax_harvesting.router, prefix="/api/tax-harvesting", tags=["tax-harvesting"])
app.include_router(optimization.router, prefix="/api/optimization", tags=["optimization"])
app.include_router(rebalancing.router, prefix="/api/rebalancing", tags=["rebalancing"])
app.include_router(reporting.router, prefix="/api/reporting", tags=["reporting"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Tax-Aware Portfolio Management API",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}

