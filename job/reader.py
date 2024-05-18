from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from core.logging import logger


class DataSourceStrategy(ABC):
    @abstractmethod
    def get_dataframe(self, spark: SparkSession, file_path: str, params: dict = None):
        pass


class CSVDataSourceStrategy(DataSourceStrategy):
    def get_dataframe(self, spark: SparkSession, file_path: str, params: dict = None):
        return (
            spark.read.format("csv")
            .option("header", True)
            .option("mergeSchema", "true")
            .load(file_path)
        )


class ParquetDataSourceStrategy(DataSourceStrategy):
    def get_dataframe(self, spark: SparkSession, file_path: str, params: dict = None):
        return (
            spark.read.format("parquet")
            .option("mergeSchema", "true")
            .option("overwriteSchema", "true")
            .load(file_path)
        )


class DeltaDataSourceStrategy(DataSourceStrategy):
    def get_dataframe(self, spark: SparkSession, file_path: str, params: dict = None):
        table_name = file_path
        return spark.read.table(tableName=table_name)


class JSONDataSourceStrategy(DataSourceStrategy):
    def get_dataframe(self, spark: SparkSession, file_path: str, params: dict = None):
        return spark.read.json(file_path)


class SQLDataSourceStrategy(DataSourceStrategy):
    def get_dataframe(self, spark: SparkSession, file_path: str, params: dict = None):
        if params is None:
            raise ValueError(
                f"Params is required"
                f"{'server': 'dbserver', 'username': 'username', 'password': 'password', 'table': 'table'}"
            )

        server = params.get("server")
        username = params.get("username")
        password = params.get("password")
        tablename = params.get("table")

        return spark.read.jdbc(
            f"jdbc:postgresql://{server}",
            f"{tablename}",
            properties={"user": f"{username}", "password": f"{password}"},
        )


data_source_strategies = {
    "csv": CSVDataSourceStrategy,
    "parquet": ParquetDataSourceStrategy,
    "delta": DeltaDataSourceStrategy,
    "json": JSONDataSourceStrategy,
    "sql": SQLDataSourceStrategy,
}


def get_data_source(data_type, file_path, spark, params=None):
    if data_type not in data_source_strategies:
        raise ValueError(f"Not implemented for data type: {data_type}")
    strategy = data_source_strategies[data_type]()
    dataframe = strategy.get_dataframe(spark, file_path, params)

    if dataframe is None:
        logger.error(
            f"get_dataframe returned None for data_type: {data_type}, file_path: {file_path}"
        )
    return dataframe
