from abc import ABC, abstractmethod


class LoadStrategy(ABC):
    """
    Strategy interface for loading DataFrame.
    """

    @abstractmethod
    def load(self, df, path, mode, params=None):
        pass


class LoadToDBFS(LoadStrategy):

    def load(self, df, path, mode, params=None):
        df.write.mode(mode).save(path)


class LoadToParquetWithPartition(LoadStrategy):

    def load(self, df, path, mode, params=None):
        partition_by_columns = params.get("partitionByColumns", [])
        df.write.format("parquet").mode(mode).partitionBy(*partition_by_columns).save(
            path
        )


class LoadToDeltaTable(LoadStrategy):

    def load(self, df, path, mode, params=None):
        df.write.format("delta").mode(mode).saveAsTable(path)


class DataSink:
    """
    Context class that uses a LoadStrategy to load data.
    """

    def __init__(self, load_strategy: LoadStrategy):
        self.load_strategy = load_strategy

    def load_dataframe(self, df, path, mode, params=None):
        self.load_strategy.load(df, path, mode, params)


def get_sink(type, df, path, mode, params=None):
    if type == "dbfs":
        strategy = LoadToDBFS()
    elif type == "parquet_with_partition":
        strategy = LoadToParquetWithPartition()
    elif type == "delta":
        strategy = LoadToDeltaTable()
    else:
        raise ValueError(f"Not implemented for sink type: {type}")

    return DataSink(strategy)