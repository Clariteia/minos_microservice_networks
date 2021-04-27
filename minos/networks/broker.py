"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import datetime

import aiopg
from aiokafka import (
    AIOKafkaProducer,
)
from aiomisc.service.periodic import (
    PeriodicService,
    Service,
)
from minos.common import (
    Aggregate,
    MinosConfig,
)
# FIXME: This import must be `minos.common` after the next `minos.common` update.
from minos.common.broker import (
    Command,
    Event,
    MinosBaseBroker,
)


class MinosBrokerDatabase:
    def get_connection(self, conf):
        conn = aiopg.connect(
            database=conf.events.queue.database,
            user=conf.events.queue.user,
            password=conf.events.queue.password,
            host=conf.events.queue.host,
            port=conf.events.queue.port,
        )
        return conn


class MinosEventBroker(MinosBaseBroker):
    ACTION = "event"

    def __init__(self, topic: str, config: MinosConfig):
        self.config = config
        self.topic = topic
        self._database()

    def _database(self):
        pass

    async def send(self, model: Aggregate):
        event_instance = Event(topic=self.topic, model=model.classname, items=[model])
        bin_data = event_instance.avro_bytes

        async with MinosBrokerDatabase().get_connection(self.config) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO producer_queue (topic, model, retry, action, creation_date, update_date) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
                    (
                        event_instance.topic,
                        bin_data,
                        0,
                        self.ACTION,
                        datetime.datetime.now(),
                        datetime.datetime.now(),
                    ),
                )

                queue_id = await cur.fetchone()
                affected_rows = cur.rowcount

        return affected_rows, queue_id[0]


class MinosCommandBroker(MinosBaseBroker):
    ACTION = "command"

    def __init__(self, topic: str, config: MinosConfig):
        self.config = config
        self.topic = topic
        self._database()

    def _database(self):
        pass

    async def send(self, model: Aggregate, reply_on: str):
        event_instance = Command(topic=self.topic, model=model.classname, items=[model], reply_on=reply_on)
        bin_data = event_instance.avro_bytes

        async with MinosBrokerDatabase().get_connection(self.config) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO producer_queue (topic, model, retry, action, creation_date, update_date) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
                    (
                        event_instance.topic,
                        bin_data,
                        0,
                        self.ACTION,
                        datetime.datetime.now(),
                        datetime.datetime.now(),
                    ),
                )

                queue_id = await cur.fetchone()
                affected_rows = cur.rowcount

        return affected_rows, queue_id[0]


async def broker_table_creation(config: MinosConfig):
    async with MinosBrokerDatabase().get_connection(config) as connect:
        async with connect.cursor() as cur:
            await cur.execute(
                'CREATE TABLE IF NOT EXISTS "producer_queue" ("id" BIGSERIAL NOT NULL PRIMARY KEY, '
                '"topic" VARCHAR(255) NOT NULL, "model" BYTEA NOT NULL, "retry" INTEGER NOT NULL, '
                '"action" VARCHAR(255) NOT NULL, "creation_date" TIMESTAMP NOT NULL, "update_date" TIMESTAMP NOT NULL);'
            )


class BrokerDatabaseInitializer(Service):
    async def start(self):
        # Send signal to entrypoint for continue running
        self.start_event.set()

        await broker_table_creation(self.config)

        await self.stop(self)


async def send_to_kafka(topic: str, message: bytes, config: MinosConfig):
    flag = False
    producer = AIOKafkaProducer(
        bootstrap_servers="{host}:{port}".format(host=config.events.broker.host, port=config.events.broker.port)
    )
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        await producer.send_and_wait(topic, message)
        flag = True
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

    return flag


async def broker_queue_dispatcher(config: MinosConfig):
    async with MinosBrokerDatabase().get_connection(config) as connect:
        async with connect.cursor() as cur:
            await cur.execute(
                "SELECT * FROM producer_queue WHERE retry <= %d ORDER BY creation_date ASC LIMIT %d;"
                % (config.events.queue.retry, config.events.queue.records),
            )
            async for row in cur:
                sent_to_kafka = False
                try:
                    sent_to_kafka = await send_to_kafka(topic=row[1], message=row[2], config=config)
                    if sent_to_kafka:
                        # Delete from database If the event was sent successfully to Kafka.
                        async with connect.cursor() as cur2:
                            await cur2.execute("DELETE FROM producer_queue WHERE id=%d;" % row[0])
                except:
                    sent_to_kafka = False
                finally:
                    if not sent_to_kafka:
                        # Update queue retry column. Increase by 1.
                        async with connect.cursor() as cur3:
                            await cur3.execute("UPDATE producer_queue SET retry = retry + 1 WHERE id=%d;" % row[0])


class EventBrokerQueueDispatcher(PeriodicService):
    async def callback(self):
        await broker_queue_dispatcher(self.config)
