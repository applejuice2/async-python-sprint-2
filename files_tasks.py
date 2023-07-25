import os
import shutil

from job import Job
from scheduler import Scheduler


def task_1():
    """Создание файла и запись данных"""
    with open('file1.txt', 'w') as f:
        f.write('Hello, World!')
        yield


def task_2():
    """Чтение данных из файла"""
    with open('file1.txt', 'r') as f:
        f.read()
        yield


def task_3():
    """Добавление данных в файл"""
    with open('file1.txt', 'a') as f:
        f.write('\nAdded line.')
        yield


def task_4():
    """Копирование файла"""
    shutil.copy('file1.txt', 'file1_copy.txt')
    yield


def task_5():
    """Перемещение файла"""
    os.makedirs('new_dir', exist_ok=True)
    shutil.move('file1_copy.txt', 'new_dir/')
    yield


def task_6():
    """Удаление файла"""
    os.remove('file1.txt')
    yield


def task_7():
    """Создание множества файлов"""
    for i in range(5):
        with open(f'file_{i}.txt', 'w') as f:
            f.write(f'This is file {i}')
            yield


def task_8():
    """Чтение из множества файлов"""
    for i in range(5):
        with open(f'file_{i}.txt', 'r') as f:
            f.read()
            yield


def task_9():
    """Изменение данных в множестве файлов"""
    for i in range(5):
        with open(f'file_{i}.txt', 'a') as f:
            f.write(f'\nAdded line to file {i}.')
            yield


def task_10():
    """Удаление множества файлов"""
    for i in range(5):
        os.remove(f'file_{i}.txt')
        yield


def main():
    job1 = Job(target=task_1,
               max_running_time=5)
    job2 = Job(target=task_2,
               max_running_time=5,
               dependencies=[job1.id])
    job3 = Job(target=task_3,
               max_running_time=5,
               dependencies=[job1.id])
    job4 = Job(target=task_4,
               max_running_time=5,
               dependencies=[job1.id])
    job5 = Job(target=task_5,
               max_running_time=5,
               dependencies=[job4.id])
    job6 = Job(target=task_6,
               max_running_time=0.1,
               dependencies=[job1.id])
    job7 = Job(target=task_7,
               max_running_time=10)
    job8 = Job(target=task_8,
               max_running_time=7,
               dependencies=[job7.id])
    job9 = Job(target=task_9,
               max_running_time=8,
               dependencies=[job7.id])
    job10 = Job(target=task_10,
                max_running_time=0.1,
                dependencies=[job7.id])

    scheduler = Scheduler(pool_size=10)
    tasks = (job1, job2, job3, job4, job5, job6, job7, job8, job9, job10)

    scheduler.process_all_tasks(tasks)


if __name__ == '__main__':
    main()
