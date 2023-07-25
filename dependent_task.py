import time

from job import Job
from scheduler import Scheduler


def task_1():
    """Сбор данных"""
    for _ in range(3):
        time.sleep(1)
        yield


def task_2():
    """Обработка данных"""
    for _ in range(2):
        time.sleep(1)
        yield


def task_3():
    """Анализ данных"""
    for _ in range(3):
        time.sleep(1)
        yield


def task_4():
    """Сохранение результатов"""
    for _ in range(2):
        time.sleep(1)
        yield


def task_5():
    """Отправка уведомления о завершении"""
    for _ in range(2):
        time.sleep(1)
        yield


def main():
    job1 = Job(target=task_1, max_running_time=5)
    job2 = Job(target=task_2, max_running_time=5, dependencies=[job1.id])
    job3 = Job(target=task_3, max_running_time=5, dependencies=[job2.id])
    job4 = Job(target=task_4, max_running_time=5, dependencies=[job3.id])
    job5 = Job(target=task_5, max_running_time=5, dependencies=[job4.id])

    scheduler = Scheduler(pool_size=8)
    tasks = (job1, job2, job3, job4, job5)

    scheduler.process_all_tasks(tasks)


if __name__ == '__main__':
    main()
