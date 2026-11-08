from sqlalchemy import create_engine, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class Jobs(Base):
    __tablename__ = "jobs"

    job_id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True
    )
    status: Mapped[str] = mapped_column(
        String(36),
        default="queued",
        nullable=False
    )
    attempts: Mapped[int] = mapped_column(
        default=0,
        nullable=False
    )
    filename: Mapped[str] = mapped_column(
        String(255),
        nullable=False
    )
