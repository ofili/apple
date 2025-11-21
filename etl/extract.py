from job.extractor import DataSourceConfig, MultiSourceExtractor
from core.logging.logger import logger


class CustomerTransactionsExtractor(MultiSourceExtractor):
    def __init__(self, spark):
        super().__init__(
            spark, 
            configs=[
            DataSourceConfig("csv", "data/customer.csv"),
            DataSourceConfig("csv", "data/transaction.csv"),
            DataSourceConfig("csv", "data/products.csv")
            ],
            data_source_names=["customer", "transaction", "product"]
        )

    def extract(self):
        extracted_data = super().extract()

        if extracted_data is None:
            logger.error("Failed to extract customer and transactions data")
            return None
        
        customer_data = extracted_data.get("customer")
        if customer_data is None:
            logger.error("Failed to extract customer data")
            return None

        transactions_data = extracted_data.get("transaction")
        if transactions_data is None:
            logger.error("Failed to extract transactions data")
            return None
        
        product_data = extracted_data.get("product")
        if product_data is None:
            logger.error("Failed to extract products data")
            return None

        result = {
            "customers": customer_data,
            "transactions": transactions_data,
            "products": product_data
        }
        logger.info("Extracted customer and transactions data")
        return result
