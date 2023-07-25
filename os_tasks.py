import time
import os

from job import Job
from scheduler import Scheduler


def task_1():
    """Создание директории и запись в файлы"""
    if not os.path.exists('test_dir'):
        os.makedirs('test_dir')
        yield
    for i in range(1, 6):
        with open(f'test_dir/test_file_{i}.txt', 'w') as f:
            f.write(f"This is test file {i}.")
        yield
        time.sleep(1)


def task_2():
    """Чтение из файлов"""
    for i in range(1, 4):
        if os.path.exists(f'test_dir/test_file_{i}.txt'):
            with open(f'test_dir/test_file_{i}.txt', 'r') as f:
                print(f.read())
            yield
            time.sleep(1)


def task_3():
    """Изменение содержимого файлов"""
    for i in range(1, 6):
        if os.path.exists(f'test_dir/test_file_{i}.txt'):
            with open(f'test_dir/test_file_{i}.txt', 'a') as f:
                f.write(f"\nAppended text to file {i}.")
            yield
            time.sleep(1)


def task_4():
    """Удаление файлов"""
    for i in range(1, 5):
        time.sleep(0.5)
        if os.path.exists(f'test_dir/test_file_{i}.txt'):
            os.remove(f'test_dir/test_file_{i}.txt')
            yield


def task_5():
    """Удаление директории"""
    time.sleep(1)
    if os.path.exists('test_dir'):
        os.rmdir('test_dir')
    yield


def main():
    job1 = Job(target=task_1,
               max_running_time=10,
               id=1)

    job2 = Job(target=task_2,
               max_running_time=6,
               dependencies=[job1.id],
               id=2)

    job3 = Job(target=task_3,
               max_running_time=10,
               dependencies=[job1.id],
               id=3)

    job4 = Job(target=task_4,
               max_running_time=4,
               dependencies=[job3.id],
               id=4)

    job5 = Job(target=task_5,
               max_running_time=3,
               dependencies=[job4.id],
               id=5)

    scheduler = Scheduler(pool_size=3)
    task_list = [job1, job2, job3, job4, job5]

    scheduler.process_all_tasks(task_list)


if __name__ == '__main__':
    main()
