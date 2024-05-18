from core.spark_context import spark_context
from etl.jobs import AirpodsAfterIphoneETLJob
from job.workflow import Workflow


if __name__ == "__main__":

    spark = spark_context.get_spark_session()

    job = AirpodsAfterIphoneETLJob(spark)
    workflow = Workflow(job)

    workflow.runner()

    spark_context.stop_spark_session()