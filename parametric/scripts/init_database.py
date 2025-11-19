#!/usr/bin/env python3
"""
Initialize the database by creating all tables.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.core.config import Config
from src.core.database import create_database_engine, init_database

def main():
    print("Initializing database...")
    print(f"Database URL: {Config.get_database_url()}")
    engine = create_database_engine(Config.get_database_url(), echo=False)
    init_database(engine)
    print("Database initialized successfully!")

if __name__ == "__main__":
    main()
