import time
from typing import Callable

from exceptions import TaskTimeLimitError


def measure_execution_time(func: Callable):
    """
    Декоратор для измерения времени выполнения функции и проверки на
    превышение максимального времени.

    Если функция превысит установленное для неё максимальное время выполнения
    (указано в max_running_time объекта), будет выброшено исключение
    TaskTimeLimitError.

    Args:
        func (Callable): Функция или метод, время выполнения которого
        требуется измерять.

    Returns:
        Callable: Обёрнутая функция или метод с контролем времени выполнения.
    """

    def wrapper(self, *args, **kwargs):
        # Если у задачи нет ограничения по времени, просто выполняем метод run
        if not self.max_running_time:
            return func(self, *args, **kwargs)

        # Если есть ограничение, то засекаем время исполнения задачи
        start_time = time.time()
        result = func(self, *args, **kwargs)
        passed_time = time.time() - start_time

        self.running_time += passed_time

        if self.running_time > self.max_running_time:
            print(f'Функция превысела максимально возможное время исполнения '
                  f'| {self.max_running_time} > {self.running_time}')
            raise TaskTimeLimitError

        return result

    return wrapper
