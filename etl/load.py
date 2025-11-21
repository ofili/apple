from job.load import Loader
from job.loader import get_sink


class AirpodsAfterIphoneLoader(Loader):

    def save(self):
         params = {"partitionByColumns": ["location"]}

         get_sink(
            type="parquet_with_partition",
            df=self.dataframe,
            path="output/airpods_after_iphone",
            mode="overwrite",
            params=params,
        ).load_dataframe(
            df=self.dataframe,
            path="output/airpods_after_iphone",
            mode="overwrite",
            params=params,
        )
         

class AirpodsAndIphoneLoader(Loader):
    def save(self):
        params = {"partitionByColumns": ["location"]}

        get_sink(
            type="dbfs",
            df=self.dataframe,
            path="airpods_and_iphone_delta",
            mode="overwrite",
            # params=params,
        ).load_dataframe(
            df=self.dataframe,
            path="airpods_and_iphone_delta",
            mode="overwrite",
            # params=params,
        )


class ProductsAfterInitialPurchaseLoader(Loader):
    def save(self):

        get_sink(
            type="dbfs",
            df=self.dataframe,
            path="output/products_after_initial_purchase",
            mode="overwrite",
        ).load_dataframe(
            df=self.dataframe,
            path="output/products_after_initial_purchase",
            mode="overwrite",
        )