import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

path_root = Path(__file__).resolve().parent.parent
sys.path.append(str(path_root))

from core.config import settings
from core.logging.logger import logger


class SparkContextManager:
    """
    A class to manage the SparkContext and SparkSession.
    """

    def __init__(self):
        self._spark = None
        self.spark_app_name = settings.get_config("spark", "app_name")
        self.master = settings.get_config("DEFAULT", "master")

        try:
            self._spark = (
                SparkSession.builder.appName(self.spark_app_name)
                .master(self.master)
                .config(
                        "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
                    )
                .config(
                    "spark.hadoop.fs.s3a.endpoint",
                    os.getenv("S3_ENDPOINT", "s3.amazonaws.com"),
                )
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .getOrCreate()
            )

            # Configure Spark for AWS access (if applicable)
            # if self.aws_access_key_id and self.aws_secret_access_key:
            #     logger.info("Setting AWS credentials for S3 access")
            #     self._spark.conf.set(
            #         "spark.hadoop.fs.s3a.access.key", self.aws_access_key_id
            #     )
            #     self._spark.conf.set(
            #         "spark.hadoop.fs.s3a.secret.key", self.aws_secret_access_key
            #     )
            # else:
            #     logger.warning(
            #         "AWS access key ID or secret key not found in configuration file"
            #     )

            logger.info("SparkSession created")
        
        except Exception as e:
            logger.error(f"Failed to create SparkSession: {e}")
            raise
    
    def get_spark_session(self) -> SparkSession:
        if not self._spark:
            raise RuntimeError("SparkSession has not been initialized.")
        return self._spark

    def stop_spark_session(self):
        """
        Explicitly stops the SparkSession.
        """
        if self._spark:
            self._spark.stop()
            logger.info("SparkSession stopped")

    def __del__(self):
        """
        Closes the SparkSession.
        """
        if self._spark:
            try:
                self._spark.stop()
                logger.info("SparkSession stopped")
            except Exception as e:
                logger.error(f"Error stopping SparkSession: {e}")


spark_context = SparkContextManager()
