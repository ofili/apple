from job.interface import ETLJob


class Workflow:
    """
    Executes ETL workflows based on provided job implementations.
    """

    def __init__(self, job: ETLJob):
        """Initializes a new instance of the Workflow class.

        :param job: The job implementation to run.
        """
        self.job = job

    def runner(self):
        self.job.run()