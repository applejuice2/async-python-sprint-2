import pickle
import time
from collections import deque
from uuid import UUID

from job import (Job,
                 JobStatus)
from exceptions import (TaskTimeLimitError,
                        QueueFullOfElems)


class Scheduler:
    """
    Планировщик задач.

    Отвечает за выполнение задач с учётом их зависимостей,
    максимального времени выполнения и других ограничений.
    Позволяет управлять задачами: запускать, останавливать,
    перезапускать и сохранять состояние.

    Атрибуты:
        queue (deque[Job, maxlen=10]): Очередь задач на выполнение
        с максимальной длиной 10.
        dependency_map (dict[UUID, list[Job]]): Словарь задач-зависимостей.
        file (str): Путь к файлу для сохранения состояния планировщика.
    """

    def __init__(self, pool_size=10, file='jobs.pkl'):
        self.queue: deque[Job] = deque(maxlen=pool_size)
        self.file: str = file
        self.dependency_map = {}
        print('Шедулер инициализирован')

    def process_all_tasks(self, tasks: tuple[Job]) -> None:
        """
        Добавляет и обрабатывает все переданные задачи, учитывая ограничение
        на максимальное количество одновременно выполняемых задач,
        установленное параметром `pool_size`.

        Параметры:
            tasks (tuple[Job]): Кортеж задач для добавления и выполнения.
        """
        idx = 0

        while idx < len(tasks):
            try:
                self.__schedule(tasks[idx])
                # Индекс инкрементируется после успешного добавления задачи.
                idx += 1
            except QueueFullOfElems:
                # Если очередь заполнена, выполняем одну итерацию планировщика.
                try:
                    next(self.run())
                except StopIteration:
                    # Если все задачи обработаны, завершаем работу.
                    break

        # Запускаем оставшиеся задачи в планировщике.
        for _ in self.run():
            pass

    def __schedule(self, task: Job) -> None:
        """
        Добавляет задачу в очередь на выполнение.
        Если задача имеет зависимости, инициирует службу зависимостей.

        Args:
            task (Job): Задача для добавления в очередь.

        Raises:
            QueueFullOfElems: Если очередь задач полна.
        """
        if len(self.queue) < self.queue.maxlen:
            self.queue.appendleft(task)
            print(f'{task.id} добавлена в очередь')
            if task.dependencies:
                self.__start_dependency_service(task, self.dependency_map)
        else:
            raise QueueFullOfElems

    def run(self) -> None:
        """
        Основной метод для исполнения всех задач в очереди.
        """
        while len(self.queue):
            task = self.queue[-1]

            if (self.___task_should_not_start(task, self.queue)
                    or self.__dependencies_not_complete(task, self.queue)):
                continue

            self.__set_task_status_to_started(task)

            try:
                print(f'Выполнение {task.id}')
                task.run()
            except StopIteration:
                task.status = JobStatus.FINISHED
                self.__delete_dependency_task_from_map(task)
                self.queue.pop()
                yield
                continue
            except (TaskTimeLimitError, Exception) as e:
                if task.max_restarts > task.restarts:
                    task.restarts += 1
                    self.queue.rotate(1)
                else:
                    task.status = JobStatus.FAILED
                    self.__delete_dependency_task_from_map(task)
                    self.queue.pop()
                    yield
                print(f'Ошибка при выполнении {task.id}: {e}')
                continue

            self.queue.rotate(1)

        print('Все задачи обработаны')

    def restart(self):
        """
        Перезапускает все задачи и возвращает их в исходное состояние.
        """
        with open(self.file, 'rb') as file:
            loaded_tasks = pickle.load(file)

        for task in loaded_tasks:
            task.status = JobStatus.NOT_STARTED
            self.__schedule(task)

        self.run()

    def stop(self):
        """
        Останавливает все задачи, сохраняя их текущее состояние в файл.
        """
        for task in self.queue:
            task.pause()

        with open(self.file, 'wb') as file:
            pickle.dump(self.queue, file, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def __update_dependency_status(task: Job, dependency_task: Job) -> None:
        """
        Обновляет статус зависимой задачи внутри основной задачи.

        Если основная задача имеет зависимую задачу и эта зависимая задача
        была выполнена, этот метод обновляет статус зависимой задачи внутри
        основной задачи.

        Args:
            task (Job): Основная задача.
            dependency_task (Job): Зависимая задача.
        """
        if dependency_task.id in task.dependency_statuses:
            task.dependency_statuses[dependency_task.id] = (
                dependency_task.status
            )

    @staticmethod
    def __start_dependency_service(
        task: Job,
        dependency_map: dict[UUID, list[Job]]
    ) -> None:
        """
        Добавляет зависимые задачи в словарь dependency_map.

        Если основная задача имеет зависимости, этот метод добавляет их
        в словарь для отслеживания.

        Args:
            task (Job): Основная задача.
            dependency_map (dict[UUID, list[Job]]): Словарь для отслеживания
            зависимостей.
        """
        for dependency in task.dependencies:
            print(f'Найдена зависимая задача - {dependency}. '
                  'Ожидание выполнения...')
            if dependency not in dependency_map:
                dependency_map[dependency] = []
            dependency_map[dependency].append(task)

    @staticmethod
    def ___task_should_not_start(task: Job, queue: deque[Job]) -> bool:
        """
        Проверяет, следует ли начинать выполнение задачи
        на основе времени старта.

        Args:
            task (Job): Задача для проверки.
            queue (deque[Job]): Текущая очередь задач.

        Returns:
            bool: Возвращает True, если задача не
            должна начинаться, иначе False.
        """
        if task.start_at and time.time() < task.start_at:
            print(f'Задача {task.id} отложена. Время начала ещё не наступило.')
            queue.rotate(1)
            return True
        return False

    @staticmethod
    def __dependencies_not_complete(task: Job, queue: deque[Job]) -> bool:
        """
        Проверяет, завершены ли все зависимые задачи для основной задачи.

        Args:
            task (Job): Основная задача.
            queue (deque[Job]): Текущая очередь задач.

        Returns:
            bool: Возвращает True, если зависимые
            задачи не завершены, иначе False.
        """
        dependencies_completed = all(
            [status in [JobStatus.FINISHED, JobStatus.FAILED]
             for status in task.dependency_statuses.values()]
        )
        if not dependencies_completed:
            print(f'Задача {task.id} отложена из-за '
                  'невыполненных зависимостей.')
            task.status = JobStatus.POSTPONED
            queue.rotate(1)
            return True
        return False

    @staticmethod
    def __set_task_status_to_started(task: Job) -> None:
        """
        Устанавливает статус задачи в "STARTED", если она ещё не была начата.

        Args:
            task (Job): Задача для изменения статуса.
        """
        if task.status != JobStatus.STARTED:
            task.status = JobStatus.STARTED

    def __delete_dependency_task_from_map(self, task: Job) -> None:
        """
        Удаляет задачу из словаря отслеживания
        зависимостей после её выполнения.

        После успешного завершения задачи, удаляет её из словаря зависимостей и
        обновляет статусы всех задач, которые от неё зависели.

        Args:
            task (Job): Задача для удаления.
        """
        if task.id in self.dependency_map:
            dependent_tasks = self.dependency_map[task.id]
            for dependent_task in dependent_tasks:
                self.__update_dependency_status(dependent_task, task)
            del self.dependency_map[task.id]
