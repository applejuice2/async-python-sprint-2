import requests

from job import Job
from scheduler import Scheduler


def task_1():
    """Запрос к простому API"""
    requests.get("https://jsonplaceholder.typicode.com/todos/1")
    yield


def task_2():
    """Обработка ответа от API"""
    data = requests.get("https://jsonplaceholder.typicode.com/todos/2")
    yield
    data.json()
    yield


def task_3():
    """Повторные запросы к API"""
    urls = ["https://jsonplaceholder.typicode.com/todos/2",
            "https://jsonplaceholder.typicode.com/todos/3",
            "https://jsonplaceholder.typicode.com/todos/4"]
    for url in urls:
        response = requests.get(url)
        response.json()
        yield


def task_4():
    """Запрос с задержкой"""
    response = requests.get("https://httpbin.org/delay/2")
    response.json()
    yield


def task_5():
    """Получение изображения с сервера"""
    response = requests.get("https://www.example.com/image.jpg")
    with open("downloaded_image.jpg", "wb") as f:
        f.write(response.content)
    yield


def task_6():
    """Запрос к несуществующему серверу (Ошибку обработает Планировщик)"""
    requests.get("http://thisurldoesnotexist.xyz")
    yield


def task_7():
    """Запрос с анализом заголовков"""
    response = requests.get("https://jsonplaceholder.typicode.com/todos/1")
    response.headers.get("Content-Type")
    yield


def task_8():
    """Отправка данных на сервер (POST-запрос)"""
    response = requests.post("https://httpbin.org/post", data={"key": "value"})
    response.json()
    yield


def main():
    job1 = Job(target=task_1, max_running_time=5)
    job2 = Job(target=task_2, max_running_time=5, dependencies=[job1.id])
    job3 = Job(target=task_3, max_running_time=10)
    # Эта завершится с ошибкой из-за задержки в 2 секунды
    job4 = Job(target=task_4, max_running_time=1)
    job5 = Job(target=task_5, max_running_time=10)
    job6 = Job(target=task_6, max_running_time=5)
    job7 = Job(target=task_7, max_running_time=5)
    job8 = Job(target=task_8, max_running_time=5)

    scheduler = Scheduler(pool_size=8)
    tasks = (job1, job2, job3, job4, job5, job6, job7, job8)

    scheduler.process_all_tasks(tasks)


if __name__ == '__main__':
    main()
