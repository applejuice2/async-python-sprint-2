import time

from job import Job
from scheduler import Scheduler


def task_1():
    """Сбор данных"""
    print("Сбор данных...")
    for _ in range(3):
        time.sleep(1)
        print("Данные собраны частично...")
        yield


def task_2():
    """Обработка данных"""
    print("Обработка данных...")
    for _ in range(2):
        time.sleep(1)
        print("Данные обработаны частично...")
        yield


def task_3():
    """Анализ данных"""
    print("Анализ данных...")
    for _ in range(3):
        time.sleep(1)
        print("Данные проанализированы частично...")
        yield


def task_4():
    """Сохранение результатов"""
    print("Сохранение результатов...")
    for _ in range(2):
        time.sleep(1)
        print("Результаты сохранены частично...")
        yield


def task_5():
    """Отправка уведомления о завершении"""
    print("Отправка уведомления...")
    for _ in range(2):
        time.sleep(1)
        print("Уведомление отправлено частично...")
        yield


def main():
    job1 = Job(target=task_1, max_running_time=5, id=1)
    job2 = Job(target=task_2, max_running_time=5, dependencies=[job1.id], id=2)
    job3 = Job(target=task_3, max_running_time=5, dependencies=[job2.id], id=3)
    job4 = Job(target=task_4, max_running_time=5, dependencies=[job3.id], id=4)
    job5 = Job(target=task_5, max_running_time=5, dependencies=[job4.id], id=5)

    scheduler = Scheduler(pool_size=8)
    task_list = [job1, job2, job3, job4, job5]

    scheduler.process_all_tasks(task_list)


if __name__ == '__main__':
    main()
