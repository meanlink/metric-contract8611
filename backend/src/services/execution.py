"""多路执行的租约认领、心跳续租、条件发布与中断自动恢复。

每次对账只有持有有效租约（claim_token 匹配且状态为 running）的执行端能发布结果；
租约过期或历史遗留的处理中任务由 recover_expired 自动恢复重新排队续算；
迟到返回的旧执行端发布会被拒绝并留痕，不覆盖现行结果也不回写失败。
"""
import json, time, uuid
from sqlalchemy import select, update
from src.config.database import Session
from src.models import Job, JobAttempt
from src.services.audit import record
from src.utils.serialization import checksum

LEASE_SECONDS = 30      # 认领租约时长；执行中按心跳在期内续租
HEARTBEAT_INTERVAL = 5  # 心跳间隔，须明显小于租约时长
REQUIRED_RESULT_KEYS = ('summary', 'metrics', 'table', 'chart', 'findings', 'method')


def claim(db, job_id, executor):
    """原子认领排队中的任务；成功返回 (token, attempt_id)，已被他方持有则返回 None。"""
    now = time.time()
    token = f'{executor}:{uuid.uuid4().hex}'
    res = db.execute(
        update(Job).where(Job.id == job_id, Job.status == 'queued')
        .values(status='running', claim_token=token, claim_expires_at=now + LEASE_SECONDS))
    if res.rowcount != 1:
        db.rollback()
        return None
    attempt = JobAttempt(job_id=job_id, executor=executor, claimed_at=now, heartbeat_at=now)
    db.add(attempt)
    db.commit()
    return token, attempt.id


def heartbeat(job_id, token, attempt_id):
    """续租并记录心跳；租约已丢失（被恢复或他方接手）时返回 False。"""
    now = time.time()
    with Session() as db:
        res = db.execute(
            update(Job).where(Job.id == job_id, Job.claim_token == token, Job.status == 'running')
            .values(claim_expires_at=now + LEASE_SECONDS))
        db.execute(
            update(JobAttempt).where(JobAttempt.id == attempt_id, JobAttempt.outcome == 'running')
            .values(heartbeat_at=now))
        db.commit()
        return res.rowcount == 1


def publish(job_id, token, attempt_id, result=None, error=None):
    """仅当租约仍由本执行端持有时发布结果；迟到发布被丢弃并留痕，返回是否生效。"""
    now = time.time()
    if error is None:
        json.dumps(result, allow_nan=False)
        values = {'status': 'completed', 'result': result, 'result_checksum': checksum(result),
                  'error': '', 'finished_at': now, 'claim_token': '', 'claim_expires_at': None}
        outcome, reason = 'completed', ''
    else:
        values = {'status': 'failed', 'error': str(error)[:990], 'finished_at': now,
                  'claim_token': '', 'claim_expires_at': None}
        outcome, reason = 'failed', str(error)[:490]
    with Session() as db:
        res = db.execute(
            update(Job).where(Job.id == job_id, Job.claim_token == token, Job.status == 'running')
            .values(**values))
        if res.rowcount != 1:
            # 旧执行迟到返回：任务已被恢复或由他方完成，丢弃本次结果/失败回写，不覆盖现行状态
            note = '租约已失效或被其他执行端接手，迟到' + ('结果' if error is None else '失败回写') + '已丢弃，未覆盖现行结果'
            a = db.get(JobAttempt, attempt_id)
            if a and a.outcome == 'running':
                a.outcome = 'superseded'
                a.finished_at = now
                a.reason = note
            elif a and a.outcome == 'recovered' and '迟到' not in a.reason:
                a.reason = (a.reason + '；' + note)[:490]
            record(db, 'worker', 'job.stale', {'id': job_id, 'outcome': outcome})
            db.commit()
            return False
        db.execute(
            update(JobAttempt).where(JobAttempt.id == attempt_id, JobAttempt.outcome == 'running')
            .values(outcome=outcome, finished_at=now, reason=reason))
        detail = {'id': job_id} if error is None else {'id': job_id, 'error': str(error)[:990]}
        record(db, 'worker', 'job.' + outcome, detail)
        db.commit()
        return True


def recover_expired(db):
    """把租约过期或历史遗留的处理中任务恢复为排队续算，返回需重新入队的任务 id。"""
    now = time.time()
    stale = list(db.scalars(select(Job).where(
        Job.status == 'running',
        (Job.claim_expires_at.is_(None)) | (Job.claim_expires_at < now))))
    recovered = []
    for j in stale:
        reason = ('执行端租约过期（进程可能已中断），系统自动恢复并重新排队续算'
                  if j.claim_expires_at is not None else
                  '历史遗留的处理中任务缺少有效租约，自动恢复并重新排队')
        db.execute(
            update(JobAttempt).where(JobAttempt.job_id == j.id, JobAttempt.outcome == 'running')
            .values(outcome='recovered', finished_at=now, reason=reason))
        j.status = 'queued'
        j.claim_token = ''
        j.claim_expires_at = None
        record(db, 'worker', 'job.recovered', {'id': j.id, 'reason': reason})
        recovered.append(j.id)
    return recovered


def current_result(job):
    """批准与分发只接受完整且校验通过的一份结果；历史已完成结果无指纹时按原样采信。"""
    if not job or job.status != 'completed' or not isinstance(job.result, dict) or not job.result:
        return None
    if any(key not in job.result for key in REQUIRED_RESULT_KEYS):
        return None
    if job.result_checksum:
        try:
            if checksum(job.result) != job.result_checksum:
                return None
        except (TypeError, ValueError):
            return None
    return job.result
