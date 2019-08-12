import functools
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import metadata, Queue, Task, Token
import operations as ops


if __name__ == "__main__":
    # Setup
    engine = create_engine("sqlite:///:memory:",
    # echo=True
    )
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    get_queue = functools.partial(ops.get_queue, session)
    create_queue = functools.partial(ops.create_queue, session)
    get_or_create_queue = functools.partial(ops.get_or_create_queue, session)
    create_task = functools.partial(ops.create_task, session)
    get_task = functools.partial(ops.get_task, session)
    task_ack = functools.partial(ops.task_ack, session)
    task_nack = functools.partial(ops.task_nack, session)
    # get_or_create_bucket = functools.partial(ops.get_or_create_bucket, session)
    # create_tokens = functools.partial(ops.create_tokens, session)
    create_queue_tokens = functools.partial(ops.create_queue_tokens, session)

    # Create a queue with some tasks
    create_queue("Foo", 500, 1)
    create_task("Bar", get_queue("Foo"))
    create_task("Baz", get_queue("Foo"))
    all_queues = session.query(Queue).all()
    print(all_queues)
    all_tasks = session.query(Task).all()
    print(all_tasks)

    # No tasks available from queue as no work tokens have yet been generated
    task_0 = get_task(get_queue("Foo"))
    print(task_0)
    assert task_0 is None


    # Wait so that some tokens are generated
    # Should create 5(ish) tokens
    time.sleep(5)
    create_queue_tokens(get_queue("Foo"))
    tokens = session.query(Token).filter(Token.queue == get_queue("Foo")).all()
    print(tokens)
    assert len(tokens) == 5

    # Get first two tasks
    task_1 = get_task(get_queue("Foo"))
    print(task_1)
    assert task_1.payload == "Bar"
    task_2 = get_task(get_queue("Foo"))
    print(task_2)
    assert task_2.payload == "Baz"

    # Mark first task as incomplete
    task_nack(task_1)

    # Next task will be the first task again
    task_3 = get_task(get_queue("Foo"))
    print(task_3)
    assert task_1.payload == "Bar"

    # Mark both tasks as complete
    task_ack(task_1)
    task_ack(task_2)

    # No more tasks in queue
    task_4 = get_task(get_queue("Foo"))
    print(task_4)
    assert task_4 is None


# Scheduler cycle
# 1. Generate tokens for all queues
# 2. For all queues get and runs tasks
# 3. Repeat
