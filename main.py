from core.spark_context import spark_context
from etl.jobs import AirpodsAfterIphoneETLJob, AirpodsAndIphoneETLJob, ProductsAfterInitialPurchaseETLJob
from job.workflow import Workflow


if __name__ == "__main__":

    spark = spark_context.get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    airpods_after_iphone = AirpodsAfterIphoneETLJob(spark)
    airpods_and_iphone = AirpodsAndIphoneETLJob(spark)
    products_after_initial_purchase = ProductsAfterInitialPurchaseETLJob(spark)

    # Create a workflow instance
    workflow = Workflow()

    # Add jobs to the workflow
    workflow.add_job(airpods_after_iphone)
    workflow.add_job(airpods_and_iphone)
    workflow.add_job(products_after_initial_purchase)

    # Run all jobs in the workflow
    workflow.run_all_jobs()

    # Stop the Spark session when done
    spark_context.stop_spark_session()