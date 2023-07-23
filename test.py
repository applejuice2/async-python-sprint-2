import time
from job import Job
from scheduler import Scheduler

def task_1():
    for i in range(3):
        print("Task 1 running: Step", i+1)
        time.sleep(1)
        yield

def task_2():
    for i in range(2):
        print("Task 2 running: Step", i+1)
        time.sleep(1.5)
        yield

def task_3():
    for i in range(2):
        print("Task 3 running: Step", i+1)
        time.sleep(2)
        yield

def task_4():
    for i in range(2):
        print("Task 4 running: Step", i+1)
        time.sleep(0.5)
        yield

job1 = Job(target=task_1, max_running_time=5, id=1) # будет работать 3 секунды, но лимит 5 секунд, все ок
job2 = Job(target=task_2, max_running_time=6, id=2) # будет работать 3 секунды, лимит 6 секунд, все ок
job3 = Job(target=task_3, max_running_time=3, dependencies=[job1.id, job2.id], id=3) # зависит от job1 и job2, будет работать 4 секунды, но лимит 3 секунды
job4 = Job(target=task_4, max_running_time=2, id=4) # будет работать 1 секунду, но лимит 2 секунды, все ок

# Инициализация планировщика
scheduler = Scheduler(pool_size=3)

# Добавление задач в планировщик
scheduler.schedule(job1)
scheduler.schedule(job2)
scheduler.schedule(job3)
scheduler.schedule(job4)  # Этот задача не будет добавлена из-за ограничения размера планировщика
scheduler.run()

import time
print(time.time())

