from etl.extract import CustomerTransactionsExtractor
from etl.load import AirpodsAfterIphoneLoader, AirpodsAndIphoneLoader, ProductsAfterInitialPurchaseLoader
from etl.transform import AirpodsAfterIphoneTransform, AirpodsAndIphoneTransform, ProductsAfterInitialPurchaseTransform
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


class AirpodsAndIphoneETLJob(ETLJob):
    def __init__(self, spark):
        self.spark = spark

    def run(self):
        customer_transactions = CustomerTransactionsExtractor(
            spark=self.spark,
        ).extract()

        transformed_customer_transactions = AirpodsAndIphoneTransform().transform(
            customer_transactions
        )

        AirpodsAndIphoneLoader(transformed_customer_transactions).save()


class ProductsAfterInitialPurchaseETLJob(ETLJob):
    def __init__(self, spark):
        self.spark = spark

    def run(self):
        customer_transactions = CustomerTransactionsExtractor(
            spark=self.spark,
        ).extract()

        transformed_customer_transactions = ProductsAfterInitialPurchaseTransform().transform(
            customer_transactions
        )

        ProductsAfterInitialPurchaseLoader(transformed_customer_transactions).save()
        