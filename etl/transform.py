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


class AirpodsAndIphoneTransform(Transformer):
    def transform(self, dataframes):
        """
        Customers who have bought Airpods and iPhone
        :param dataframes: Input dataframes
        :return:
        """
        transactions = dataframes.get("transactions")
        
        agg_transactions = transactions.groupBy("customer_id").agg(
            f.collect_set("product_name").alias("products")
        )

        filtered_agg_transactions = agg_transactions.where(
            (f.size(agg_transactions["products"]) == 2)
            & f.array_contains(agg_transactions["products"], "iPhone")
            & f.array_contains(agg_transactions["products"], "AirPods")
        )

        customers = dataframes.get("customers")
        customer_transactions = customers.join(
            f.broadcast(filtered_agg_transactions), "customer_id"
        )

        return customer_transactions.select("customer_id", "customer_name", "location")
    

class ProductsAfterInitialPurchaseTransform(Transformer):
  def transform(self, dataframes):
    """
    Products purchased by customers after their initial purchase

    :param dataframes: Input dataframes (dictionary)
    :return: DataFrame containing subsequent purchases with product details
    """
    transactions = dataframes.get("transactions")
    customers = dataframes.get("customers")
    products = dataframes.get("products")

    customer_transactions = customers.join(
        f.broadcast(transactions), "customer_id"
    )

    # Cast join_date to date type before finding minimum
    first_purchase_df = customer_transactions.groupBy("customer_id").agg(
        f.first(f.col("join_date").cast("date")).alias("first_purchase_date")
    )

    # Join transactions and first_purchase_df to identify subsequent purchases
    subsequent_purchases = customer_transactions.join(
        first_purchase_df, on="customer_id", how="inner"
    ).filter("transaction_date > first_purchase_date")

    subsequent_purchases.show()
    products.show()

    sp = subsequent_purchases.alias("sp")
    p = products.alias("p")
    enriched_data = sp.join(p, on="product_name", how="inner") \
    .select("sp.customer_id", "sp.product_name", "p.price", "p.category")
    
    enriched_data.show()

    return enriched_data