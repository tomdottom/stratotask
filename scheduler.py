import asyncio

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import metadata, Token
from operations import (
    get_all_queues,
    get_queue,
    create_queue_tokens,
    get_task,
    create_queue,
    create_task,
    task_ack,
)


async def run_task(task):
    task_ack(session, task)
    print(task.payload)


async def main(session):
    # Scheduler cycle
    while True:
        # 1. Generate tokens for all queues
        queues = get_all_queues(session)
        for q in queues:
            create_queue_tokens(session, q)
        # 2. For all queues get and runs tasks
        for q in queues:
            # get task
            task = get_task(session, q)
            if task:
                await run_task(task)

        # 3. Repeat
        await asyncio.sleep(0.1)


if __name__ == "__main__":
    # Setup
    engine = create_engine(
        "sqlite:///:memory:",
        # echo=True
    )
    metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Foo can process one task per second
    create_queue(session, "Foo", 10, 1)
    create_task(session, "Foo Bar", get_queue(session, "Foo"))
    create_task(session, "Foo Baz", get_queue(session, "Foo"))
    create_task(session, "Foo Qux", get_queue(session, "Foo"))
    create_task(session, "Foo Wat", get_queue(session, "Foo"))

    # Bar twice as fast a Foo
    create_queue(session, "Bar", 10, 0.5)
    create_task(session, "Bar Bar", get_queue(session, "Bar"))
    create_task(session, "Bar Baz", get_queue(session, "Bar"))
    create_task(session, "Bar Qux", get_queue(session, "Bar"))
    create_task(session, "Bar Wat", get_queue(session, "Bar"))

    asyncio.run(main(session))
