import time

from exceptions import TaskTimeLimitError


def measure_execution_time(func):
    """Декоратор для проверки времени работы функции"""
    def wrapper(self, *args, **kwargs):
        # Если у задачи нет ограничения по времени, просто выполняем метод run
        if self.max_running_time == -1:
            return func(self, *args, **kwargs)

        start_time = time.time()
        result = func(self, *args, **kwargs)
        elapsed_time = time.time() - start_time
        
        self.running_time += elapsed_time

        if self.running_time > self.max_running_time:
            print((f"Task exceeded its allowed execution time of {self.max_running_time} seconds."))
            raise TaskTimeLimitError
        
        return result

    return wrapper