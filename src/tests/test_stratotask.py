import functools
import time
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import stratotask.operations as ops_
from stratotask.models import Queue, Task, Token, metadata


@pytest.fixture
def session():
    engine = create_engine(
        "sqlite:///:memory:",
        # echo=True
    )
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


@pytest.fixture
def ops(session):
    return SimpleNamespace(
        get_queue=functools.partial(ops_.get_queue, session),
        create_queue=functools.partial(ops_.create_queue, session),
        get_or_create_queue=functools.partial(ops_.get_or_create_queue, session),
        create_task=functools.partial(ops_.create_task, session),
        get_task=functools.partial(ops_.get_task, session),
        task_ack=functools.partial(ops_.task_ack, session),
        task_nack=functools.partial(ops_.task_nack, session),
        create_queue_tokens=functools.partial(ops_.create_queue_tokens, session),
    )


def test_create_some_tasks(session, ops):
    ops.create_queue("Foo", 500, 1)
    ops.create_task("Bar", ops.get_queue("Foo"))
    ops.create_task("Baz", ops.get_queue("Foo"))
    all_queues = session.query(Queue).all()
    assert len(all_queues) == 1
    all_tasks = session.query(Task).all()
    assert tuple(t.payload for t in all_tasks) == ("Bar", "Baz")


def test_does_not_get_task_when_no_tokens_available(session, ops):
    ops.create_queue("Foo", 500, 1)
    ops.create_task("Bar", ops.get_queue("Foo"))
    task_0 = ops.get_task(ops.get_queue("Foo"))
    assert task_0 is None


def test_tokens_are_created_as_time_passes(session, ops):
    ops.create_queue("Foo", 5, 0.001)
    time.sleep(0.01)
    ops.create_queue_tokens(ops.get_queue("Foo"))
    tokens = session.query(Token).filter(Token.queue == ops.get_queue("Foo")).all()
    assert len(tokens) == 5


def test_gets_tasks(session, ops):
    ops.create_queue("Foo", 5, 0.001)
    ops.create_task("Bar", ops.get_queue("Foo"))
    ops.create_task("Baz", ops.get_queue("Foo"))
    time.sleep(0.01)
    ops.create_queue_tokens(ops.get_queue("Foo"))
    task_1 = ops.get_task(ops.get_queue("Foo"))
    assert task_1.payload == "Bar"
    task_2 = ops.get_task(ops.get_queue("Foo"))
    assert task_2.payload == "Baz"


def test_mark_task_as_incomplete(session, ops):
    ops.create_queue("Foo", 5, 0.001)
    ops.create_task("Bar", ops.get_queue("Foo"))
    time.sleep(0.01)
    ops.create_queue_tokens(ops.get_queue("Foo"))

    task_1 = ops.get_task(ops.get_queue("Foo"))
    assert task_1.payload == "Bar"
    task_2 = ops.get_task(ops.get_queue("Foo"))
    assert task_2 is None
    ops.task_nack(task_1)
    task_3 = ops.get_task(ops.get_queue("Foo"))
    assert task_3.payload == "Bar"


def test_mark_task_as_complete(session, ops):
    ops.create_queue("Foo", 5, 0.001)
    ops.create_task("Bar", ops.get_queue("Foo"))
    time.sleep(0.01)
    ops.create_queue_tokens(ops.get_queue("Foo"))

    task_1 = ops.get_task(ops.get_queue("Foo"))
    assert task_1.payload == "Bar"
    ops.task_ack(task_1)
    task_2 = ops.get_task(ops.get_queue("Foo"))
    assert task_2 is None
