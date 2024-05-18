from job.load import Loader
from job.loader import get_sink


class AirpodsAfterIphoneLoader(Loader):

    def save(self):
         params = {"partitionByColumns": ["location"]}

         get_sink(
            type="parquet_with_partition",
            df=self.dataframe,
            path="/home/ofili/projects/sparkapps/apple_analytics/data/output/airpods_after_iphone",
            mode="overwrite",
            params=params,
        ).load_dataframe(
            df=self.dataframe,
            path="/home/ofili/projects/sparkapps/apple_analytics/data/output/airpods_after_iphone",
            mode="overwrite",
            params=params,
        )