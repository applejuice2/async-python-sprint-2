from typing import (Optional,
                    Callable)
from enum import Enum
from uuid import (UUID,
                  uuid4)

from utils import measure_execution_time


class JobStatus(Enum):
    """
    Перечисление, представляющее статусы задачи.

    Атрибуты:
        NOT_STARTED: Задача ещё не была запущена.
        STARTED: Задача в процессе выполнения.
        PAUSED: Задача приостановлена.
        POSTPONED: Задача отложена (обычно из-за невыполненных зависимостей).
        FAILED: Задача завершилась с ошибкой.
        FINISHED: Задача успешно выполнена.
    """

    NOT_STARTED = 'NOT STARTED'
    STARTED = 'STARTED'
    PAUSED = 'PAUSED'
    POSTPONED = 'POSTPONED'
    FAILED = 'FAILED'
    FINISHED = 'FINISHED'


class Job:
    """
    Представляет запланированную задачу с определёнными настройками,
    зависимостями и ограничениями выполнения.

    Атрибуты:
        id (UUID): Уникальный идентификатор задачи.
        args (tuple): Аргументы для целевого генератора.
        kwargs (dict): Именованные аргументы для целевого генератора.
        start_at (int, optional): Временная метка начала выполнения задачи.
        По умолчанию задача выполняется сразу.
        running_time (int): Накопленное время выполнения задачи.
        max_running_time (int): Максимально допустимое время выполнения;
        None означает отсутствие ограничений.
        restarts (int): Количество перезапусков задачи.
        max_restarts (int): Максимально допустимое количество перезапусков.
        dependencies (list[UUID]): Список ID задач,
        от которых зависит данная задача.
        dependency_statuses (dict[UUID, JobStatus]): Статусы зависимых задач.
        status (JobStatus): Текущий статус задачи.
        coroutine (Generator): Целевая корутина для выполнения задачи.

    Методы:
        run(): Запускает или возобновляет выполнение корутины задачи.
        pause(): Временно приостанавливает выполнение задачи.
        finish(): Отмечает задачу как завершённую.
    """

    def __init__(self,
                 target: Callable,
                 id: Optional[UUID] = None,
                 args: Optional[tuple] = None,
                 kwargs: Optional[dict] = None,
                 start_at: Optional[int] = None,
                 running_time: int = 0,
                 max_running_time: Optional[int] = None,
                 restarts: int = 0,
                 max_restarts: int = 0,
                 dependencies: Optional[list[UUID]] = None,
                 dependency_statuses: Optional[dict[UUID, JobStatus]] = None,
                 status: JobStatus = JobStatus.NOT_STARTED) -> None:

        self.id = id or uuid4()
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.start_at = start_at
        self.running_time = running_time
        self.max_running_time = max_running_time
        self.restarts = restarts
        self.max_restarts = max_restarts
        self.dependencies = dependencies or []
        self.dependency_statuses = (
            dependency_statuses or {dep: JobStatus.NOT_STARTED
                                    for dep in self.dependencies}
        )
        self.status = status
        self.coroutine = target(*self.args, **self.kwargs)

    @measure_execution_time
    def run(self):
        self.status = JobStatus.STARTED
        self.coroutine.send(None)

    def pause(self):
        self.status = JobStatus.PAUSED

    def finish(self):
        self.status = JobStatus.FINISHED
