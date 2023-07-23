from collections import deque
import pickle
import time

from job import (Job,
                 JobStatus)
from exceptions import TaskTimeLimitError


class Scheduler:
    def __init__(self, pool_size=10, file = 'jobs.pkl'):
        self.queue: deque[Job] = deque(maxlen=pool_size)
        self.file: str = file
        self.dependency_map = {}
        print('Шедулер инициализирован')

    # def schedule(self, task: Job):
    #     if len(self.queue) < self.queue.maxlen:
    #         self.queue.appendleft(task)
    #     else:
    #         print('Queue is full')
    def schedule(self, task: Job):
        if len(self.queue) < self.queue.maxlen:
            self.queue.appendleft(task)
            # dependency service start
            print(f'{task.id} добавлена в очередь')
            for dependency in task.dependencies:
                if dependency not in self.dependency_map:
                    self.dependency_map[dependency] = []
                self.dependency_map[dependency].append(task)
            # dependency service end
        else:
            print('Queue is full')

    def run(self):
        while len(self.queue):
            task = self.queue[-1]

            # Проверка времени старта
            if task.start_at and time.time() < task.start_at:
                self.queue.rotate(1)
                continue

            # Проверка статусов задач-зависимостей
            dependencies_completed = all(
                [status in [JobStatus.FINISHED, JobStatus.FAILED] for status in task.dependency_statuses.values()]
            )
            if not dependencies_completed:
                # Здесь можно добавить логирование, что задача отложена из-за не выполненных зависимостей
                print(f'Задача {task.id} отложена из-за того что для неё есть невыполненные задачи.')
                task.status = JobStatus.POSTPONED
                self.queue.rotate(1)
                continue

            if task.status != JobStatus.STARTED:
                task.status = JobStatus.STARTED
            
            try:
                print(f'{task.id} стартовала!')
                task.run()


            # check time -> check dependency tasks -> edit task status -> task.run -> exceptions services
            # Корректное завершение задачи
            except StopIteration:
                task.status = JobStatus.FINISHED
                if task.id in self.dependency_map:
                    dependent_tasks = self.dependency_map[task.id]
                    for dependent_task in dependent_tasks:
                        self.update_dependency_status(dependent_task, task.id, task.status)
                    del self.dependency_map[task.id]
                print(task.id, 'успешно выполнена')
                self.queue.pop()
                print(task.id, 'удалена из очереди')
                continue

            # Некорректное завершение задачи по времени
            except (TaskTimeLimitError, Exception):
                print(task.id, 'ошибка')
                if task.max_restarts > task.restarts:
                    task.restarts += 1
                    self.queue.rotate(1)
                else:
                    task.status = JobStatus.FAILED
                    if task.id in self.dependency_map:
                        dependent_tasks = self.dependency_map[task.id]
                        for dependent_task in dependent_tasks:
                            self.update_dependency_status(dependent_task, task.id, task.status)
                        del self.dependency_map[task.id]

                    self.queue.pop()
                continue
    
            # Задача не завршена, все ок, начинается другая задача генератор
            self.queue.rotate(1)

    def restart(self):
        with open(self.file, 'rb') as file:
            loaded_tasks = pickle.load(file)
        for task in loaded_tasks:
            task.status = JobStatus.NOT_STARTED
            self.schedule(task)
        self.run()

    def stop(self):
        for task in self.queue:
            task.pause()

        with open(self.file, 'wb') as file:
            pickle.dump(self.queue, file, protocol=pickle.HIGHEST_PROTOCOL)
    
    @staticmethod
    def update_dependency_status(task, dependency_task_id, dependency_task_status):
        if dependency_task_id in task.dependency_statuses:
            task.dependency_statuses[dependency_task_id] = dependency_task_status
