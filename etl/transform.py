from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import Window

from job.transformer import Transformer


class AirpodsAfterIphoneTransform(Transformer):

    def transform(self, dataframes):
        """
        Customers who have bought Airpods after buying the iPhone
        :param dataframes: Input dataframes
        :return:
        """
        transactions = dataframes.get("transactions")
        print(transactions.show())

        window_spec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformed_transactions = transactions.withColumn(
            "next_product_name", f.lead("product_name").over(window_spec)
        )

        transformed_transactions.orderBy(
            "customer_id", "transaction_date", "product_name"
        ).show()

        filtered_transformed_transactions = transformed_transactions.filter(
            (f.col("product_name") == "iPhone")
            & (f.col("next_product_name") == "AirPods")
        )

        customers = dataframes.get("customers")

        customer_transactions = customers.join(
            f.broadcast(filtered_transformed_transactions), "customer_id"
        )

        customer_transactions.orderBy(
            "customer_id", "transaction_date", "product_name"
        ).show()

        return customer_transactions.select("customer_id", "customer_name", "location")


def products_array(product_array):
    return [p.lower() for p in product_array]


products_array_udf = f.udf(products_array, ArrayType(StringType()))
