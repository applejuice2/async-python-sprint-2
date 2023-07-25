import time

from job import Job
from scheduler import Scheduler


def task_1():
    for i in range(3):
        time.sleep(1)
        yield


def task_2():
    for i in range(2):
        time.sleep(1.5)
        yield


def task_3():
    for i in range(2):
        time.sleep(2)
        yield


def task_4():
    for i in range(2):
        time.sleep(0.5)
        yield


def task_5():
    for i in range(2):
        time.sleep(1)
        yield


def task_6():
    for i in range(3):
        time.sleep(1.5)
        yield


def task_7():
    for i in range(4):
        time.sleep(0.5)
        yield


def main():
    # будет работать 3 секунды, но лимит 5 секунд, все ок
    job1 = Job(target=task_1,
               max_running_time=5,
               id=1)
    # будет работать 3 секунды, лимит 6 секунд, все ок
    job2 = Job(target=task_2,
               max_running_time=6,
               id=2)
    # зависит от job1 и job2, будет работать 4 секунды, но лимит 3 секунды
    job3 = Job(target=task_3,
               max_running_time=3,
               dependencies=[job1.id, job2.id],
               id=3)
    # будет работать 1 секунду, но лимит 2 секунды, все ок
    job4 = Job(target=task_4,
               max_running_time=2,
               id=4)
    # будет работать 2 секунды, лимит 3 секунды, все ок
    job5 = Job(target=task_5,
               max_running_time=3,
               id=5)
    # будет работать 4.5 секунды, лимит 6 секунды, все ок
    job6 = Job(target=task_6,
               max_running_time=6,
               id=6)
    # будет работать 2 секунды, лимит 3 секунды, все ок
    job7 = Job(target=task_7,
               max_running_time=3,
               id=7)

    scheduler = Scheduler(pool_size=3)
    task_list = [job1, job2, job3, job4, job5, job6, job7]

    scheduler.process_all_tasks(task_list)


if __name__ == '__main__':
    main()
