from typing import (Optional, 
                    Generator)
from enum import Enum
from uuid import (UUID, 
                  uuid4)

from utils import measure_execution_time


class JobStatus(Enum):
    NOT_STARTED = 'NOT STARTED'
    STARTED = 'STARTED'
    PAUSED = 'PAUSED'
    POSTPONED = 'POSTPONED'
    FAILED = 'FAILED'
    FINISHED = 'FINISHED'


class Job:
    def __init__(self,
                 target: Generator, 
                 id: UUID = None,
                 args: Optional[tuple] = None, 
                 kwargs: Optional[dict] = None, 
                 start_at: Optional[int ] = None,
                 running_time: int = 0,
                 max_running_time: int = -1,
                 restarts: int = 0,
                 max_restarts: int = 0, 
                 dependencies: Optional[list['Job.id']] = None,
                 dependency_statuses = None,
                 status: JobStatus = JobStatus.NOT_STARTED) -> None:

        self.id = id or uuid4()
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.start_at = start_at
        self.running_time = running_time
        self.max_running_time = max_running_time
        self.restarts = restarts
        self.max_restarts = max_restarts
        self.dependencies = dependencies or []
        self.dependency_statuses = dependency_statuses or {dep: JobStatus.NOT_STARTED for dep in self.dependencies} 
        self.status = status
        self.coroutine = target(*self.args, **self.kwargs)

    @measure_execution_time
    def run(self):
        self.coroutine.send(None)

    def pause(self):
        self.status = JobStatus.PAUSED

    def finish(self):
        self.status = JobStatus.FINISHED

    # def to_dict(self) -> dict:
    #     return {
    #         'id': str(self.id),
    #         'target': self.target.__name__,
    #         'args': self.args,
    #         'kwargs': self.kwargs,
    #         'start_at': self.start_at,
    #         'running_time': self.running_time,
    #         'max_running_times': self.max_running_time,
    #         'tries': self.tries,
    #         'max_tries': self.max_tries,
    #         'dependencies': [str(task_id) for task_id in self.dependencies],
    #         'status': self.status.value
    #     }


    # @classmethod
    # def from_dict(cls, data: dict) -> 'Job':
    #     data['id'] = UUID(data['id'])
    #     data['status'] = JobStatus(data['status'])
    #     data['dependencies'] = [UUID(task_id) for task_id in data['dependencies']]
    #     # вот здесь я хочу преобразовывать data['id'] к uuid и также с dependencies

# a = JobStatus.FAILED.value
# print(repr(a))
# print(repr(JobStatus('FAILED')))
# print(JobStatus.FAILED == JobStatus(a))
