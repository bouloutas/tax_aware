"""
Benchmark data management.
"""
from datetime import date
from decimal import Decimal
from typing import Optional
import pandas as pd
from sqlalchemy.orm import Session
from src.core.database import Benchmark, BenchmarkConstituent

class BenchmarkManager:
    """Manager for benchmark operations."""
    def __init__(self, session: Session):
        self.session = session
    
    def create_benchmark(self, benchmark_name: str, benchmark_type: str = "index") -> Benchmark:
        benchmark = Benchmark(benchmark_name=benchmark_name, benchmark_type=benchmark_type)
        self.session.add(benchmark)
        self.session.commit()
        self.session.refresh(benchmark)
        return benchmark
    
    def get_benchmark(self, benchmark_id: int):
        return self.session.query(Benchmark).filter(Benchmark.benchmark_id == benchmark_id).first()
    
    def get_benchmark_by_name(self, benchmark_name: str):
        return self.session.query(Benchmark).filter(Benchmark.benchmark_name == benchmark_name).first()
    
    def add_constituent(self, benchmark_id: int, security_id: int, weight: Decimal,
                        effective_date: date) -> BenchmarkConstituent:
        existing = self.session.query(BenchmarkConstituent).filter(
            BenchmarkConstituent.benchmark_id == benchmark_id,
            BenchmarkConstituent.security_id == security_id,
            BenchmarkConstituent.effective_date == effective_date).first()
        if existing:
            existing.weight = weight
            self.session.commit()
            return existing
        constituent = BenchmarkConstituent(benchmark_id=benchmark_id, security_id=security_id,
                                          weight=weight, effective_date=effective_date)
        self.session.add(constituent)
        self.session.commit()
        self.session.refresh(constituent)
        return constituent
    
    def get_benchmark_weights(self, benchmark_id: int, effective_date: Optional[date] = None) -> pd.DataFrame:
        query = self.session.query(BenchmarkConstituent).filter(
            BenchmarkConstituent.benchmark_id == benchmark_id)
        if effective_date:
            query = query.filter(BenchmarkConstituent.effective_date == effective_date)
        else:
            max_date = self.session.query(BenchmarkConstituent.effective_date).filter(
                BenchmarkConstituent.benchmark_id == benchmark_id).order_by(
                BenchmarkConstituent.effective_date.desc()).first()
            if max_date:
                query = query.filter(BenchmarkConstituent.effective_date == max_date[0])
        constituents = query.all()
        if not constituents:
            return pd.DataFrame(columns=["security_id", "ticker", "weight"])
        from src.core.database import Security
        data = []
        for const in constituents:
            security = self.session.query(Security).filter(
                Security.security_id == const.security_id).first()
            data.append({"security_id": const.security_id,
                        "ticker": security.ticker if security else None,
                        "weight": float(const.weight)})
        return pd.DataFrame(data)
