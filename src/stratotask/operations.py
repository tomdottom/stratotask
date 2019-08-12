import datetime

from sqlalchemy.exc import OperationalError

from . import exceptions as excs
from .models import Queue, Task, Token


def refresh_object(session, obj):
    session.expire(obj)
    session.refresh(obj)


def get_queue(session, name):
    return session.query(Queue).filter(Queue.name == name).first()


def get_all_queues(session):
    return session.query(Queue).all()


def create_queue(session, name, bucket_size=500, bucket_rate=1):
    queue = session.query(Queue).filter(Queue.name == name).first()
    if queue:
        raise excs.ExistingObjectError("Queue already exists")
    queue = Queue(name=name, bucket_size=bucket_size, bucket_rate=bucket_rate)
    session.add(queue)
    try:
        session.commit()
    except OperationalError:
        session.rollback()
        queue = None
    return queue


def get_or_create_queue(session, name):
    queue = get_queue(session, name)
    if not queue:
        queue = create_queue(session, name)
    return queue


def create_task(session, payload, queue):
    task = Task(payload, queue)
    session.add(task)
    try:
        session.commit()
    except OperationalError:
        session.rollback()
        task = None
    return task


def get_task(session, queue):
    refresh_object(session, queue)
    token = get_queue_token(session, queue)
    if token:
        task = (
            session.query(Task)
            .filter(Task.queue_id == queue.id)
            .filter(Task.state == Task.State.WAITING)
            .order_by(Task.scheduled)
            .first()
        )
        if task:
            try:
                task.state = Task.State.RUNNING
                session.add(task)
                session.commit()
                consume_queue_token(session, token)
            except OperationalError:
                session.rollback()
                return_queue_token(session, token)
                task = None
    else:
        task = None

    return task


def task_ack(session, task):
    refresh_object(session, task)
    task.state = Task.State.COMPLETED
    session.add(task)
    try:
        session.commit()
    except OperationalError:
        session.rollback()
        return False
    return True


def task_nack(session, task):
    refresh_object(session, task)
    task.state = Task.State.WAITING
    session.add(task)
    try:
        session.commit()
    except OperationalError:
        session.rollback()
        return False
    return True


def create_queue_tokens(session, queue):
    refresh_object(session, queue)
    now = datetime.datetime.utcnow()
    token_count = session.query(Token).filter(Token.queue == queue).count()
    # Full do nothing but update 'updated
    if token_count >= queue.bucket_size:
        try:
            queue.bucket_updated = now
            session.add(queue)
            session.commit()
        except OperationalError:
            # ?
            session.rollback()
    # calculate how many more tokens would have been created since last bucket update
    token_num, leftover_seconds = divmod(
        (now - queue.bucket_updated).total_seconds(), queue.bucket_rate
    )
    if token_num >= queue.bucket_size:
        token_num = queue.bucket_size
        leftover_seconds = 0
    try:
        queue.bucket_updated = now - datetime.timedelta(seconds=leftover_seconds)
        session.add(queue)
        for _ in range(int(token_num)):
            session.add(Token(queue))
        session.commit()
    except OperationalError:
        # ?
        session.rollback()


def get_queue_token(session, queue):
    refresh_object(session, queue)
    token = (
        session.query(Token)
        .filter(Token.queue == queue)
        .filter(Token.state == Token.State.ISSUED)
        .first()
    )
    if token:
        try:
            token.state = Token.State.RESERVED
            session.add(token)
            session.commit()
        except OperationalError:
            # ?
            token = None
            session.rollback()
    return token


def consume_queue_token(session, token):
    refresh_object(session, token)
    try:
        token.state = Token.State.CONSUMED
        session.add(token)
        session.commit()
    except OperationalError:
        # ?
        session.rollback()


def return_queue_token(session, token):
    refresh_object(session, token)
    try:
        token.state = Token.State.ISSUED
        session.add(token)
        session.commit()
    except OperationalError:
        # ?
        session.rollback()
