from etl.extract import CustomerTransactionsExtractor
from etl.load import AirpodsAfterIphoneLoader
from etl.transform import AirpodsAfterIphoneTransform
from job.interface import ETLJob


class AirpodsAfterIphoneETLJob(ETLJob):
    def __init__(self, spark):
        self.spark = spark

    def run(self):
        customer_transactions = CustomerTransactionsExtractor(
            spark=self.spark,
        ).extract()

        transformed_customer_transactions = AirpodsAfterIphoneTransform().transform(
            customer_transactions
        )

        AirpodsAfterIphoneLoader(transformed_customer_transactions).save()
        