#!/usr/bin/env python3
"""
tax_lots.py

Fetch your IB account executions and positions, build tax lots,
classify short-/long-term and wash-sale flags, and export to CSV/JSON.
"""

from ib_insync import IB, ExecutionFilter
import pandas as pd
from datetime import datetime, timedelta

# ─── USER SETTINGS ─────────────────────────────────────────────────────────────

IB_HOST    = '127.0.0.1'
IB_PORT    = 7497
CLIENT_ID  = 10
ACCOUNT    = 'DU4339520'

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def fetch_executions(ib, account, since):
    filt = ExecutionFilter(acctCode=account, time=since.strftime('%Y%m%d %H:%M:%S'))
    exec_details = ib.reqExecutions(filt)
    trades = []
    for ed in exec_details:
        ex = ed.execution
        tdate = datetime.strptime(ex.time, '%Y%m%d  %H:%M:%S')
        qty   = ex.shares if ex.side == 'BOT' else -ex.shares
        trades.append({
            'symbol':    ed.contract.symbol,
            'conId':     ed.contract.conId,
            'date':      tdate,
            'quantity':  qty,
            'price':     ex.price
        })
    # sort chronologically
    return sorted(trades, key=lambda t: t['date'])


def build_lots(trades):
    # Initialize buy-lots
    lots = []
    for t in trades:
        if t['quantity'] > 0:
            lots.append({
                'symbol':           t['symbol'],
                'conId':            t['conId'],
                'purchaseDate':     t['date'],
                'originalQuantity': t['quantity'],
                'remainingQuantity':t['quantity'],
                'costPerShare':     t['price'],
                'washFlag':         False
            })

    # Apply sells (FIFO) and detect wash‐sale replacements
    today = datetime.now()
    oneYearAgo = today - timedelta(days=365)
    for t in trades:
        if t['quantity'] < 0:
            saleQty   = -t['quantity']
            saleDate  = t['date']
            salePrice = t['price']
            # FIFO removal
            remaining = saleQty
            removedLots = []
            for lot in sorted(lots, key=lambda L: L['purchaseDate']):
                if lot['symbol'] == t['symbol'] and lot['remainingQuantity'] > 0:
                    remove = min(lot['remainingQuantity'], remaining)
                    lot['remainingQuantity'] -= remove
                    removedLots.append((lot, remove))
                    remaining -= remove
                    if remaining <= 0:
                        break
            if remaining > 0:
                print(f"WARNING: sold more {t['symbol']} than current lots")
            # Check for realized loss
            costBasis = sum(lot['costPerShare'] * qty for lot, qty in removedLots)
            proceeds  = saleQty * salePrice
            if proceeds < costBasis:
                # flag any lot bought within ±30 days as a wash replacement
                windowStart = saleDate - timedelta(days=30)
                windowEnd   = saleDate + timedelta(days=30)
                for lot in lots:
                    if (lot['symbol'] == t['symbol']
                        and windowStart <= lot['purchaseDate'] <= windowEnd):
                        lot['washFlag'] = True

    # Return only those lots that still have remainingQuantity > 0
    return [L for L in lots if L['remainingQuantity'] > 0]


def classify_and_export(open_lots):
    today = datetime.now()
    rows = []
    for lot in open_lots:
        age = (today - lot['purchaseDate']).days
        termFlag = 'short-term' if age < 365 else 'long-term'
        totalCost = lot['remainingQuantity'] * lot['costPerShare']
        rows.append({
            'symbol':        lot['symbol'],
            'conId':         lot['conId'],
            'purchaseDate':  lot['purchaseDate'].isoformat(),
            'quantity':      lot['remainingQuantity'],
            'costBasis':     totalCost,
            'lotType':       termFlag,
            'washFlag':      lot['washFlag'],
            'termFlag':      termFlag  # duplicate for clarity
        })

    df = pd.DataFrame(rows)

    # Export both CSV and JSON
    df.to_csv('tax_lots.csv', index=False)
    df.to_json('tax_lots.json', orient='records', date_format='iso')
    print("Exported tax_lots.csv and tax_lots.json")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    ib = IB()
    print(f"Connecting to TWS at {IB_HOST}:{IB_PORT} (clientId={CLIENT_ID})...")
    ib.connect(IB_HOST, IB_PORT, CLIENT_ID)

    since = datetime.now() - timedelta(days=365)
    print(f"Fetching executions since {since.date()} for account {ACCOUNT}...")
    trades = fetch_executions(ib, ACCOUNT, since)

    print(f"Building open lots (this may take a moment)...")
    open_lots = build_lots(trades)

    print(f"Found {len(open_lots)} open lots — classifying and exporting...")
    classify_and_export(open_lots)

    ib.disconnect()
    print("Done.")

if __name__ == '__main__':
    main()
