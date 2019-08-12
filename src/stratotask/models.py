import datetime
import enum
import math

from sqlalchemy import (
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Sequence,
    String,
    Text,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import relation
from sqlalchemy.schema import MetaData
from sqlalchemy.sql import text

class_registry = {}
metadata = MetaData()
ModelBase = declarative_base(metadata=metadata, class_registry=class_registry)


class Queue(ModelBase):

    __tablename__ = "queues"
    __table_args__ = {"sqlite_autoincrement": True}
    id = Column(
        Integer, Sequence("queue_id_sequence"), primary_key=True, autoincrement=True
    )
    name = Column(String(200), unique=True)
    bucket_size = Column(Integer)
    bucket_rate = Column(Integer)
    bucket_updated = Column(DateTime, default=datetime.datetime.utcnow)

    def __init__(self, name, bucket_size, bucket_rate):
        self.name = name
        self.bucket_size = bucket_size
        self.bucket_rate = bucket_rate

    def __repr__(self):
        return "<Queue({self.name})>".format(self=self)

    @declared_attr
    def messages(cls):
        return relation("Task", backref="queue", lazy="noload")

    @declared_attr
    def tokens(cls):
        return relation("Token", backref="queue", lazy="noload")


class Task(ModelBase):
    class State(enum.Enum):
        WAITING = "WAITING"
        RUNNING = "RUNNING"
        COMPLETED = "ERROR"

        def __str__(self):
            return f"State({self.value})"

    __tablename__ = "tasks"
    id = Column(
        Integer, Sequence("task_id_sequence"), primary_key=True, autoincrement=True
    )
    payload = Column(Text, nullable=False)
    scheduled = Column(
        DateTime,
        index=True,
        default=datetime.datetime.utcnow,
        onupdate=datetime.datetime.utcnow,
    )
    state = Column("state", Enum(State), default=State.WAITING)

    def __init__(self, payload, queue):
        self.payload = payload
        self.queue = queue

    def __repr__(self):
        return "<Message {0.id} {0.state}: {0.payload} {0.queue_id}>".format(self)

    @declared_attr
    def queue_id(self):
        return Column(
            Integer,
            ForeignKey(
                "%s.id" % class_registry["Queue"].__tablename__, name="FK_task_queue"
            ),
        )


class Token(ModelBase):
    class State(enum.Enum):
        ISSUED = "ISSUED"
        RESERVED = "RESERVED"
        CONSUMED = "CONSUMED"

        def __str__(self):
            return f"State({self.value})"

    __tablename__ = "tokens"
    id = Column(
        Integer, Sequence("tokens_id_sequence"), primary_key=True, autoincrement=True
    )
    name = Column(String(200), unique=True)
    state = Column("state", Enum(State), default=State.ISSUED)
    created = Column(DateTime, default=datetime.datetime.utcnow)

    def __init__(self, queue):
        self.queue = queue

    def __repr__(self):
        return "<Token({self.queue} - {self.created})>".format(self=self)

    @declared_attr
    def queue_id(self):
        return Column(
            Integer,
            ForeignKey(
                "%s.id" % class_registry["Queue"].__tablename__, name="FK_token_queue"
            ),
        )
