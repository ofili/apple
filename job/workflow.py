from job.interface import ETLJob

class Workflow:
    """
    Executes ETL workflows based on provided job implementations.
    """

    def __init__(self):
        """Initializes a new instance of the Workflow class."""
        self.jobs = []

    def add_job(self, job: ETLJob):
        """Adds a job to the workflow.

        :param job: The job implementation to add.
        """
        self.jobs.append(job)

    def run_all_jobs(self):
        """Runs all jobs in the workflow."""
        for job in self.jobs:
            job.run()