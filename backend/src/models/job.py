from sqlalchemy import String, JSON, Double, ForeignKey, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.config.database import Base
class Job(Base):
    __tablename__ = 'jobs'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160))
    dataset_id: Mapped[int] = mapped_column(ForeignKey('datasets.id'))
    params: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(20), default='queued')
    result: Mapped[dict] = mapped_column(JSON, default=dict)
    error: Mapped[str] = mapped_column(String(1000), default='')
    approved: Mapped[bool] = mapped_column(Boolean, default=False)
    claim_token: Mapped[str] = mapped_column(String(64), default='')
    claim_expires_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    result_checksum: Mapped[str] = mapped_column(String(64), default='')
    created_by: Mapped[int] = mapped_column(ForeignKey('users.id'))
    created_at: Mapped[float] = mapped_column(Double)
    finished_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    attempts: Mapped[list['JobAttempt']] = relationship('JobAttempt', back_populates='job', cascade='all, delete-orphan', lazy='selectin', order_by='JobAttempt.id')
class JobAttempt(Base):
    __tablename__ = 'job_attempts'
    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey('jobs.id'), index=True)
    executor: Mapped[str] = mapped_column(String(120), default='')
    claimed_at: Mapped[float] = mapped_column(Double)
    heartbeat_at: Mapped[float] = mapped_column(Double, default=0)
    finished_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    outcome: Mapped[str] = mapped_column(String(20), default='running')
    reason: Mapped[str] = mapped_column(String(500), default='')
    job: Mapped[Job] = relationship('Job', back_populates='attempts')
