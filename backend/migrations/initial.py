from sqlalchemy import inspect, select, text
from src.config.database import Base, engine, Session
from src import models
from src.utils.serialization import checksum
JOB_COLUMNS = {
    'claim_token': "ALTER TABLE jobs ADD COLUMN claim_token VARCHAR(64) NOT NULL DEFAULT ''",
    'claim_expires_at': "ALTER TABLE jobs ADD COLUMN claim_expires_at DOUBLE NULL",
    'result_checksum': "ALTER TABLE jobs ADD COLUMN result_checksum VARCHAR(64) NOT NULL DEFAULT ''",
}
def _ensure_job_columns():
    existing={c['name'] for c in inspect(engine).get_columns('jobs')}
    with engine.begin() as conn:
        for name,ddl in JOB_COLUMNS.items():
            if name not in existing:conn.execute(text(ddl))
def _backfill_completed():
    # 历史已完成对账结果原样保留，仅补登结果指纹与一条完成尝试记录
    with Session() as db:
        for j in db.scalars(select(models.Job).where(models.Job.status=='completed')):
            if j.result and not j.result_checksum:
                j.result_checksum=checksum(j.result)
            if j.result and not j.attempts:
                db.add(models.JobAttempt(job_id=j.id,executor='system',claimed_at=j.created_at,heartbeat_at=j.finished_at or j.created_at,finished_at=j.finished_at,outcome='completed',reason='迁移前已完成的对账，结果原样保留'))
        db.commit()
def migrate():
    Base.metadata.create_all(engine)
    _ensure_job_columns()
    _backfill_completed()
if __name__=='__main__':migrate()
