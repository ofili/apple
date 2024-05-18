class ETLJob:
    """
    Interface for ETL jobs. Specific job implementations must inherit from this class.
    """

    def run(self):
        raise NotImplementedError("Subclasses must implement the run method")