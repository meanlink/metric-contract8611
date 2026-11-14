import os, socket, threading, time, logging
from pathlib import Path
from sqlalchemy import select
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models import Job, Dataset, Report
from src.services.business import analyze
from src.services.reports import deliver
from src.services.audit import record
from src.services import execution
from src.services.jobs import enqueue
logging.basicConfig(level=logging.INFO)
EXECUTOR_ID=f'{socket.gethostname()}:{os.getpid()}'
def execute(job_id):
    with Session() as db:
        claimed=execution.claim(db,job_id,EXECUTOR_ID)
        if not claimed:return # 他方已持有有效租约，本端不再重复执行
        token,attempt_id=claimed
        j=db.get(Job,job_id)
        dataset_id,params=j.dataset_id,dict(j.params or {})
    stop=threading.Event()
    def beat():
        while not stop.wait(execution.HEARTBEAT_INTERVAL):
            try:
                if not execution.heartbeat(job_id,token,attempt_id):return # 租约已丢失，发布将被拒绝
            except Exception:logging.exception('Heartbeat failed')
    pulser=threading.Thread(target=beat,daemon=True);pulser.start()
    try:
        with Session() as db:
            d=db.get(Dataset,dataset_id)
            records=list(d.records) if d else None
        result=analyze(records,params)
        execution.publish(job_id,token,attempt_id,result=result)
    except Exception as e:
        logging.exception('Analysis failed')
        execution.publish(job_id,token,attempt_id,error=e)
    finally:
        stop.set();pulser.join(timeout=2)
def tick():
    with Session() as db:
        recovered=execution.recover_expired(db)
        db.commit()
    for job_id in recovered:enqueue(job_id)
    with Session() as db:
        queued=list(db.scalars(select(Job.id).where(Job.status=='queued').limit(3)))
    for job_id in queued:execute(job_id)
    with Session() as db:
        for r in db.scalars(select(Report).where(Report.enabled==True,Report.next_run<=time.time())):
            d=deliver(db,r)
            if d:record(db,'worker','report.delivered',{'id':r.id,'delivery':d.id})
        db.commit()
def main():
    r=Redis.from_url(REDIS_URL,socket_timeout=3)
    # 多路执行：启动只恢复租约过期或历史遗留的任务；健康执行端心跳未断的任务不受影响
    with Session() as db:
        recovered=execution.recover_expired(db)
        db.commit()
    for job_id in recovered:enqueue(job_id)
    while True:
        Path('/tmp/heartbeat').touch()
        try:
            item=r.blpop('analysis:queue',timeout=1)
            if item:execute(int(item[1]))
            tick()
        except Exception:
            logging.exception('Worker retry');time.sleep(2)
if __name__=='__main__':main()
