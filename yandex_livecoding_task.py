"""
Нам нужно передать данные из некоторого источника (producer) некоторому потребителю (consumer).
При этом источник отдает данные небольшими пачками (десятки записей),
а потребитель оптимальнее работает с крупными батчами (тысячи записей). Реальный пример - поставка данных из очередей
типа Kafka в базу ClickHouse.
Источник и потребитель описываются интерфейсами ниже.

**Свойства Источника (Producer)**
1. Условно бесконечный, то есть источник может вернуть бесконечное количество данных, однако в нашем случае, если данные
закончились, то выбрасывается исключение `ProducerException`.
2. Источник никогда не возвращает более `Consumer.MAX_ITEMS` записей за один вызов `next`.
3. Каждую порцию данных `Источник` требует подтверждать вызовом `commit`, чтобы после перезапуска программы не
отправлять все данные заново, поэтому в рамках одного инстанса `PipeProcessor` источник каждый раз возвращает новые
данные на каждый вызов `next()`. Но после перезапуска источник начнет с прошлой "подтвержденной" позиции с помощью
`commit`, задаваемой `CommitTag`. Поэтому каждое значение `CommitTag`, которое вернул вызов `next`, после сохранения
данных в приемнике, должно быть фиксировано вызовом `commit`, причем строго в той же последовательности, в которой их
вернул `next`.

**Свойства Приемника (Consumer)**
1. Не может обработать более `Consumer.MAX_ITEMS` за один вызов `process`.

**Что надо сделать**
Требуется реализовать функцию: `async def pipe(self)` в классе `PipeProcessor`, которая читает данные из источника,
группирует их в буфер (размером не более `Consumer.MAX_ITEMS`), а затем сохраняет в приёмник, после чего фиксирует
прогресс в источнике.

"""

import abc
import asyncio
import dataclasses
import logging

Item = str
CommitTag = int


@dataclasses.dataclass
class Result:
    items: list[Item]
    commit_tag: CommitTag


class ProducerException(Exception): ...


class Producer(abc.ABC):
    @abc.abstractmethod
    async def next(self) -> Result: ...

    @abc.abstractmethod
    async def commit(self, tag: CommitTag) -> None: ...


class Consumer(abc.ABC):
    MAX_ITEMS: int = 1000

    @abc.abstractmethod
    async def process(self, items: list[Item]) -> None: ...


class PipeProcessor:
    def __init__(self, producer: Producer, consumer: Consumer):
        self._producer = producer
        self._consumer = consumer
        self._last_result_items = []
        self._last_commit_tag = None

    async def pipe(self) -> None:
        while True:
            # code here
            _results_lst = []
            _commit_storage = []
            critical_size = self._consumer.MAX_ITEMS
            exceeded_size_flag: bool = False

            if self._last_result_items:
                _results_lst.extend(self._last_result_items)

            if self._last_commit_tag:
                _commit_storage.append(self._last_commit_tag)

            while len(_results_lst) < critical_size and not exceeded_size_flag:
                try:
                    cur_batch = await self._producer.next()
                    cur_items_lst = cur_batch.items

                    if len(_results_lst) + len(cur_items_lst) <= critical_size:
                        _results_lst.extend(cur_items_lst)
                        _commit_storage.append(cur_batch.commit_tag)

                    else:
                        exceeded_size_flag = True
                        self._last_result_items = cur_items_lst
                        self._last_commit_tag = cur_batch.commit_tag

                except ProducerException:
                    logging.warning('Smth extraordinary happens!')
                    self._last_result_items = []
                    self._last_commit_tag = None
                    return

            # processing items
            await self._consumer.process(_results_lst)

            # save results
            for cmt_tag in _commit_storage:
                await self._producer.commit(cmt_tag)


# tests below


# class StubProducer(Producer):
#     """
#     Producer stub for tests.

#     `StubProducer([20], 10)` generates 10 batches, each holding 20 items.
#     `StubProducer([10, 20, 30], 6)` generates 6 batches of sizes 10, 20, 30, 10, 20, 30.
#     """

#     def __init__(self, batch_sizes: list[int], stop_after: int):
#         self.batch_sizes = batch_sizes
#         self.stop_after = stop_after
#         self.produced_items: list[Result] = []
#         self.committed_tags: list[CommitTag] = []
#         self.current_tag: CommitTag = 0
#         self.iteration_count: int = 0

#     async def next(self) -> Result:
#         if self.iteration_count >= self.stop_after:
#             raise ProducerException("Producer stopped after configured iterations")

#         batch_size = self.batch_sizes[self.iteration_count % len(self.batch_sizes)]
#         self.current_tag += batch_size

#         produced_data: list[str] = [
#             f"item_{self.current_tag - batch_size + i}" for i in range(batch_size)
#         ]
#         result = Result(
#             produced_data,
#             self.current_tag,
#         )
#         self.produced_items.append(result)
#         self.iteration_count += 1
#         await asyncio.sleep(0.1)

#         return Result(
#             list(produced_data),
#             self.current_tag,
#         )

#     async def commit(self, tag: CommitTag) -> None:
#         if tag in self.committed_tags:
#             raise ValueError(f"Tag {tag} has already been committed")
#         self.committed_tags.append(tag)
#         await asyncio.sleep(0.1)


# class StubConsumer(Consumer):
#     """
#     Consumer stub for tests.

#     Note: MAX_ITEMS is overridden to simplify testing.
#     """

#     MAX_ITEMS = 100

#     def __init__(self):
#         self.consumed_items: list[Item] = []

#     async def process(self, items: list[Item]) -> None:
#         if len(items) > self.MAX_ITEMS:
#             raise ValueError(
#                 f"Received {len(items)} items, but MAX_ITEMS is {self.MAX_ITEMS}"
#             )
#         self.consumed_items.extend(items)
#         await asyncio.sleep(0.1)


# def assert_that_produced_and_committed_items_are_equal_to_consumed(
#     producer: StubProducer, consumer: StubConsumer
# ):
#     produced_and_committed_items: list[str] = []
#     for produced_item in producer.produced_items:
#         if produced_item.commit_tag in producer.committed_tags:
#             produced_and_committed_items.extend(produced_item.items)

#     assert len(produced_and_committed_items) > 0
#     assert len(consumer.consumed_items) > 0
#     assert len(produced_and_committed_items) == len(consumer.consumed_items)

#     assert produced_and_committed_items == consumer.consumed_items
