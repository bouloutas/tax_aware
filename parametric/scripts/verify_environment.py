#!/usr/bin/env python3
"""Verify conda environment has all required packages."""
import sys

def check_package(name, import_name=None):
    """Check if package is installed."""
    if import_name is None:
        import_name = name
    try:
        __import__(import_name)
        return True, None
    except ImportError as e:
        return False, str(e)

packages = {
    'pandas': 'pandas',
    'numpy': 'numpy',
    'scipy': 'scipy',
    'sqlalchemy': 'sqlalchemy',
    'psycopg2': 'psycopg2',
    'alembic': 'alembic',
    'cvxpy': 'cvxpy',
    'osqp': 'osqp',
    'fastapi': 'fastapi',
    'uvicorn': 'uvicorn',
    'pydantic': 'pydantic',
    'httpx': 'httpx',
    'yfinance': 'yfinance',
    'requests': 'requests',
    'dotenv': 'dotenv',
    'pytest': 'pytest',
    'pytest_asyncio': 'pytest_asyncio',
    'pytest_cov': 'pytest_cov',
}

print("Checking required packages...")
print("=" * 60)

all_ok = True
for name, import_name in packages.items():
    ok, error = check_package(name, import_name)
    status = "✓" if ok else "✗"
    print(f"{status} {name:20s}", end="")
    if not ok:
        print(f" - MISSING ({error})")
        all_ok = False
    else:
        print()

print("=" * 60)
if all_ok:
    print("✅ All packages installed!")
    sys.exit(0)
else:
    print("❌ Some packages missing. Run: pip install -r requirements.txt")
    sys.exit(1)
