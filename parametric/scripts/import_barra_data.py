#!/usr/bin/env python3
"""
Import Barra risk model data into parametric system.

Copies and transforms Barra data from ~/tax_aware/barra/exports/ to
~/tax_aware/parametric/data/raw/barra/ for use in portfolio optimization.
"""
import json
import shutil
from datetime import date
from pathlib import Path

import pandas as pd


def create_barra_directory():
    """Create barra data directory if it doesn't exist."""
    barra_dir = Path.home() / "tax_aware" / "parametric" / "data" / "raw" / "barra"
    barra_dir.mkdir(parents=True, exist_ok=True)
    return barra_dir


def find_latest_barra_release():
    """Find the latest Barra release directory."""
    barra_exports = Path.home() / "tax_aware" / "barra" / "exports" / "releases"
    
    if not barra_exports.exists():
        raise FileNotFoundError(f"Barra exports directory not found: {barra_exports}")
    
    # Find all release directories (date-based)
    release_dirs = [d for d in barra_exports.iterdir() if d.is_dir() and d.name.count("-") == 2]
    
    if not release_dirs:
        raise FileNotFoundError(f"No release directories found in {barra_exports}")
    
    # Sort by date and get latest
    release_dirs.sort(key=lambda x: x.name, reverse=True)
    latest_release = release_dirs[0]
    
    return latest_release


def copy_barra_files(source_dir: Path, target_dir: Path, release_date: str):
    """
    Copy Barra files to target directory.
    
    Args:
        source_dir: Source Barra release directory
        target_dir: Target directory in parametric system
        release_date: Release date string (YYYY-MM-DD)
    """
    files_to_copy = [
        "factor_covariance",
        "factor_returns",
        "style_exposures",
        "specific_risk",
        "portfolio_summary",
    ]
    
    copied_files = []
    
    for file_base in files_to_copy:
        # Try CSV first (more readable), then parquet
        for ext in [".csv", ".parquet"]:
            source_file = source_dir / f"{file_base}_{release_date}{ext}"
            if source_file.exists():
                target_file = target_dir / f"{file_base}_{release_date}{ext}"
                shutil.copy2(source_file, target_file)
                copied_files.append(target_file.name)
                print(f"  ✓ Copied {file_base}{ext}")
                break
    
    # Copy manifest if exists
    manifest_source = source_dir / "manifest.json"
    if manifest_source.exists():
        manifest_target = target_dir / f"manifest_{release_date}.json"
        shutil.copy2(manifest_source, manifest_target)
        copied_files.append(manifest_target.name)
        print(f"  ✓ Copied manifest.json")
    
    return copied_files


def create_data_summary(target_dir: Path, release_date: str):
    """Create a summary document of the imported Barra data."""
    summary = {
        "release_date": release_date,
        "imported_files": [],
        "data_summary": {},
    }
    
    # Check each file and summarize
    files_to_check = [
        "factor_covariance",
        "factor_returns",
        "style_exposures",
        "specific_risk",
        "portfolio_summary",
    ]
    
    for file_base in files_to_check:
        csv_file = target_dir / f"{file_base}_{release_date}.csv"
        parquet_file = target_dir / f"{file_base}_{release_date}.parquet"
        
        file_to_read = csv_file if csv_file.exists() else (parquet_file if parquet_file.exists() else None)
        
        if file_to_read:
            try:
                if file_to_read.suffix == ".csv":
                    df = pd.read_csv(file_to_read, nrows=1000)  # Sample
                else:
                    df = pd.read_parquet(file_to_read)
                    # Limit for summary
                    if len(df) > 1000:
                        df = df.head(1000)
                
                summary["imported_files"].append(file_to_read.name)
                summary["data_summary"][file_base] = {
                    "rows": len(df),
                    "columns": list(df.columns),
                    "file_size_mb": round(file_to_read.stat().st_size / (1024 * 1024), 2),
                }
            except Exception as e:
                print(f"  ⚠ Warning: Could not read {file_to_read.name}: {e}")
    
    # Save summary
    summary_file = target_dir / f"data_summary_{release_date}.json"
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)
    
    print(f"  ✓ Created data summary: {summary_file.name}")
    return summary


def create_readme(target_dir: Path, release_date: str, summary: dict):
    """Create README explaining the Barra data."""
    readme_content = f"""# Barra Risk Model Data

**Release Date**: {release_date}
**Source**: ~/tax_aware/barra/exports/releases/{release_date}/
**Imported**: {date.today().isoformat()}

## Files

"""
    
    for file_base in ["factor_covariance", "factor_returns", "style_exposures", "specific_risk", "portfolio_summary"]:
        if file_base in summary.get("data_summary", {}):
            info = summary["data_summary"][file_base]
            readme_content += f"### {file_base.replace('_', ' ').title()}\n"
            readme_content += f"- **File**: `{file_base}_{release_date}.csv` / `.parquet`\n"
            readme_content += f"- **Rows**: {info.get('rows', 'N/A')}\n"
            readme_content += f"- **Columns**: {', '.join(info.get('columns', [])[:5])}...\n"
            readme_content += f"- **Size**: {info.get('file_size_mb', 'N/A')} MB\n\n"
    
    readme_content += """## Data Description

### Factor Covariance
Covariance matrix of factor returns. Used for portfolio risk calculation.

### Factor Returns
Historical factor returns (style factors, industry factors, country factor).

### Style Exposures
Factor exposures for each security:
- Style factors: Beta, Momentum, Size, Earnings Yield, Book-to-Price, Growth, Earnings Variability, Leverage, Currency Sensitivity, Dividend Yield
- Industry factors: GICS sector/industry exposures
- Country factor: US market exposure

### Specific Risk
Idiosyncratic (stock-specific) risk for each security.

### Portfolio Summary
Summary statistics for a sample portfolio (if available).

## Usage

These files can be used by the portfolio optimization engine to:
1. Calculate portfolio risk using factor exposures
2. Estimate tracking error
3. Optimize portfolios with risk constraints

See `src/optimization/risk_model.py` for integration.
"""
    
    readme_file = target_dir / "README.md"
    with open(readme_file, "w") as f:
        f.write(readme_content)
    
    print(f"  ✓ Created README.md")


def main():
    """Main import function."""
    print("=" * 70)
    print("BARRA DATA IMPORT")
    print("=" * 70)
    
    # Find latest Barra release
    print("\n1. Finding latest Barra release...")
    try:
        source_dir = find_latest_barra_release()
        release_date = source_dir.name
        print(f"   ✓ Found release: {release_date}")
    except FileNotFoundError as e:
        print(f"   ✗ Error: {e}")
        return 1
    
    # Create target directory
    print("\n2. Creating target directory...")
    target_dir = create_barra_directory()
    print(f"   ✓ Target: {target_dir}")
    
    # Copy files
    print("\n3. Copying Barra files...")
    try:
        copied_files = copy_barra_files(source_dir, target_dir, release_date)
        print(f"   ✓ Copied {len(copied_files)} files")
    except Exception as e:
        print(f"   ✗ Error copying files: {e}")
        return 1
    
    # Create summary
    print("\n4. Creating data summary...")
    try:
        summary = create_data_summary(target_dir, release_date)
        print(f"   ✓ Summary created")
    except Exception as e:
        print(f"   ⚠ Warning: Could not create summary: {e}")
        summary = {}
    
    # Create README
    print("\n5. Creating README...")
    try:
        create_readme(target_dir, release_date, summary)
    except Exception as e:
        print(f"   ⚠ Warning: Could not create README: {e}")
    
    print("\n" + "=" * 70)
    print("✅ Barra data import complete!")
    print(f"   Release date: {release_date}")
    print(f"   Location: {target_dir}")
    print("=" * 70)
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

