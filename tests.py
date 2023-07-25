import unittest
from uuid import UUID
from time import sleep
from unittest.mock import Mock, patch

from scheduler import Scheduler
from job import Job, JobStatus
from exceptions import QueueFullOfElems
from exceptions import TaskTimeLimitError
from utils import measure_execution_time


def yield_none_generator():
    for _ in range(3):
        yield None


class TestJob(unittest.TestCase):
    def test_job_initialization(self):
        mock_function = Mock(side_effect=yield_none_generator)
        job = Job(target=mock_function)

        self.assertIsInstance(job.id, UUID)
        self.assertEqual(job.args, ())
        self.assertEqual(job.kwargs, {})
        self.assertEqual(job.running_time, 0)
        self.assertEqual(job.status, JobStatus.NOT_STARTED)
        self.assertIsNotNone(job.coroutine)

    def test_job_run(self):
        mock_function = Mock(side_effect=yield_none_generator)
        job = Job(target=mock_function)

        job.run()
        self.assertEqual(job.status, JobStatus.STARTED)

    def test_job_pause(self):
        mock_function = Mock(side_effect=yield_none_generator)
        job = Job(target=mock_function)

        job.run()
        job.pause()
        self.assertEqual(job.status, JobStatus.PAUSED)

    def test_job_finish(self):
        mock_function = Mock(side_effect=yield_none_generator)
        job = Job(target=mock_function)

        job.run()
        job.finish()
        self.assertEqual(job.status, JobStatus.FINISHED)

    def test_job_time_limit(self):
        def long_function():
            sleep(2)
            yield

        job = Job(target=long_function, max_running_time=1)

        with self.assertRaises(TaskTimeLimitError):
            job.run()


class TestMeasureExecutionTime(unittest.TestCase):
    def test_execution_time_measurement(self):
        @measure_execution_time
        def mock_job(self):
            sleep(1)

        mock_self = Mock(max_running_time=2, running_time=0)
        mock_job(mock_self)

        self.assertGreaterEqual(mock_self.running_time, 1)

    def test_time_limit_exceed(self):
        @measure_execution_time
        def mock_job(self):
            sleep(2)

        mock_self = Mock(max_running_time=1, running_time=0)

        with self.assertRaises(TaskTimeLimitError):
            mock_job(mock_self)


class TestScheduler(unittest.TestCase):

    def setUp(self):
        self.scheduler = Scheduler(pool_size=2)
        self.mock_job = Mock(spec=Job)
        self.mock_job.id = "test_id"
        self.mock_job.dependencies = []
        self.mock_job.start_at = None
        self.mock_job.dependency_statuses = {}
        self.mock_job.status = JobStatus.NOT_STARTED

    def test_initialization(self):
        self.assertEqual(len(self.scheduler.queue), 0)
        self.assertEqual(self.scheduler.file, 'jobs.pkl')
        self.assertEqual(len(self.scheduler.dependency_map), 0)

    def test_schedule_single_task(self):
        self.scheduler._Scheduler__schedule(self.mock_job)
        self.assertEqual(len(self.scheduler.queue), 1)

    def test_process_all_tasks(self):
        mock_jobs = [self.mock_job for _ in range(2)]
        with patch.object(self.scheduler, "run", return_value=[]):
            self.scheduler.process_all_tasks(tuple(mock_jobs))
        self.assertEqual(len(self.scheduler.queue), 2)

    def test_queue_full_exception(self):
        mock_queue = Mock(maxlen=2, __len__=Mock(return_value=2))
        self.scheduler.queue = mock_queue
        self.scheduler.queue.appendleft = Mock(side_effect=QueueFullOfElems)
        with self.assertRaises(QueueFullOfElems):
            self.scheduler._Scheduler__schedule(self.mock_job)

    def test_stop(self):
        with patch('builtins.open', unittest.mock.mock_open()) as mock_open:
            self.scheduler.stop()
            mock_open.assert_called_with('jobs.pkl', 'wb')

    def test_restart(self):
        self.mock_job.status = JobStatus.FINISHED
        with (patch('builtins.open', unittest.mock.mock_open(read_data=b"")),
              patch('pickle.load', return_value=[self.mock_job])):
            self.scheduler.restart()
            self.assertEqual(self.mock_job.status, JobStatus.NOT_STARTED)


if __name__ == "__main__":
    unittest.main()
