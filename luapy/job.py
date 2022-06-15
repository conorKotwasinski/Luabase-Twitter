from threading import Thread

import luapy.utils.pg_db_utils as pgu
from luapy.logger import logger


class Job(Thread):
    """Base implementation for a long running, threaded, daemonic job"""

    def __init__(self, *args, job_id=None, db=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.result = None
        self.success = None

        try:
            assert job_id is not None
        except AssertionError:
            logger.error("`job_id` must be defined!")

        try:
            assert db is not None
        except AssertionError:
            #TODO: refactor db into the pgu module so that queries can be run without explicitly passing the engine object
            logger.error("`db` must be defined!")

        self.job_id = job_id
        self.db = db

    def _job_cleanup(self, error=None):

        if error:
            logger.error(error)

        try:
            pgu.updateJobStatus(self.db.engine, {"id": self.job_id, "status": "failed"})
        except:
            logger.error(f"We couldn't set the status for job `{self.job_id}` to `failed`")

    def run(self):
        """Copied base implementation with a few tweaks to keep track of status"""
        try:
            if self._target is not None:
                self.result = self._target(*self._args, **self._kwargs)
                self.success = True
        except Exception as error:
            self.result = error
            self.success = False
            self._job_cleanup(error)
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs
