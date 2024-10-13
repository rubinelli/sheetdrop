
from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from typing import List

class Base(DeclarativeBase):
    pass 

class FileStatus(Base):
    __tablename__ = 'file_status'

    status_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, nullable=False)
    file_id: Mapped[str] = mapped_column(nullable=False)
    status: Mapped[str] = mapped_column(nullable=False)
    status_details: Mapped[List["FileStatusDetail"]] = relationship(cascade="all, delete-orphan")

class FileStatusDetail(Base):
    __tablename__ = 'file_status_detail'

    status_detail_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, nullable=False)
    status_id: Mapped[int] = mapped_column(ForeignKey("file_status.status_id"), nullable=False)
    status_detail: Mapped[str] = mapped_column(nullable=False)
