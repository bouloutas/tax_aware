"""
Position and tax lot management.
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from sqlalchemy.orm import Session
from src.core.database import Account, Position, Security, TaxLot, Transaction

class PositionManager:
    """Manager for position and tax lot operations."""
    def __init__(self, session: Session):
        self.session = session
    
    def create_tax_lot(self, account_id: int, security_id: int, purchase_date: date,
                       purchase_price: Decimal, quantity: Decimal) -> TaxLot:
        cost_basis = purchase_price * quantity
        tax_lot = TaxLot(account_id=account_id, security_id=security_id,
                        purchase_date=purchase_date, purchase_price=purchase_price,
                        quantity=quantity, cost_basis=cost_basis,
                        remaining_quantity=quantity, status="open")
        self.session.add(tax_lot)
        self.session.commit()
        self.session.refresh(tax_lot)
        self._update_position(account_id, security_id, quantity)
        self._create_transaction(account_id, security_id, "buy",
                                datetime.combine(purchase_date, datetime.min.time()),
                                quantity, purchase_price, cost_basis)
        return tax_lot
    
    def sell_from_tax_lot(self, tax_lot_id: int, sell_date: date, sell_price: Decimal,
                          quantity: Decimal, wash_sale_flag: bool = False):
        tax_lot = self.session.query(TaxLot).filter(TaxLot.tax_lot_id == tax_lot_id).first()
        if not tax_lot or quantity > tax_lot.remaining_quantity:
            raise ValueError("Invalid tax lot or quantity")
        cost_basis_sold = (tax_lot.cost_basis / tax_lot.quantity) * quantity
        proceeds = sell_price * quantity
        realized_gain_loss = proceeds - cost_basis_sold
        tax_lot.remaining_quantity -= quantity
        tax_lot.cost_basis -= cost_basis_sold
        if tax_lot.remaining_quantity == 0:
            tax_lot.status = "closed"
        transaction = self._create_transaction(tax_lot.account_id, tax_lot.security_id, "sell",
                                              datetime.combine(sell_date, datetime.min.time()),
                                              quantity, sell_price, proceeds, tax_lot_id,
                                              realized_gain_loss, wash_sale_flag)
        self._update_position(tax_lot.account_id, tax_lot.security_id, -quantity)
        self.session.commit()
        return transaction, tax_lot
    
    def get_positions(self, account_id: int) -> list[Position]:
        return self.session.query(Position).filter(Position.account_id == account_id).all()
    
    def get_position(self, account_id: int, security_id: int) -> Optional[Position]:
        return self.session.query(Position).filter(
            Position.account_id == account_id, Position.security_id == security_id).first()
    
    def get_tax_lots(self, account_id: int, security_id: Optional[int] = None,
                    status: Optional[str] = None) -> list[TaxLot]:
        query = self.session.query(TaxLot).filter(TaxLot.account_id == account_id)
        if security_id:
            query = query.filter(TaxLot.security_id == security_id)
        if status:
            query = query.filter(TaxLot.status == status)
        return query.all()
    
    def get_transactions(self, account_id: int, start_date: Optional[date] = None,
                        end_date: Optional[date] = None,
                        transaction_type: Optional[str] = None) -> list[Transaction]:
        query = self.session.query(Transaction).filter(Transaction.account_id == account_id)
        if start_date:
            query = query.filter(Transaction.transaction_date >= datetime.combine(start_date, datetime.min.time()))
        if end_date:
            query = query.filter(Transaction.transaction_date <= datetime.combine(end_date, datetime.min.time()))
        if transaction_type:
            query = query.filter(Transaction.transaction_type == transaction_type)
        return query.order_by(Transaction.transaction_date).all()
    
    def _update_position(self, account_id: int, security_id: int, quantity_delta: Decimal) -> Optional[Position]:
        position = self.get_position(account_id, security_id)
        if position:
            position.quantity += quantity_delta
            if position.quantity == 0:
                self.session.delete(position)
                self.session.commit()
                return None
        else:
            if quantity_delta > 0:
                position = Position(account_id=account_id, security_id=security_id, quantity=quantity_delta)
                self.session.add(position)
        self.session.commit()
        if position:
            self.session.refresh(position)
        return position
    
    def _create_transaction(self, account_id: int, security_id: int, transaction_type: str,
                           transaction_date: datetime, quantity: Optional[Decimal] = None,
                           price: Optional[Decimal] = None, total_amount: Optional[Decimal] = None,
                           tax_lot_id: Optional[int] = None, realized_gain_loss: Optional[Decimal] = None,
                           wash_sale_flag: bool = False) -> Transaction:
        transaction = Transaction(account_id=account_id, security_id=security_id,
                                 transaction_type=transaction_type, transaction_date=transaction_date,
                                 quantity=quantity, price=price, total_amount=total_amount,
                                 tax_lot_id=tax_lot_id, realized_gain_loss=realized_gain_loss,
                                 wash_sale_flag=wash_sale_flag)
        self.session.add(transaction)
        return transaction
