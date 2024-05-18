from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List  # Import for type annotations

from core.logging .logger import logger
from .reader import get_data_source


class DataSourceConfig:
    """
    Holds information about a data source.

    Attributes:
        data_type (str): The type of data source (e.g., "csv", "parquet").
        file_path (str): The path to the data file associated with the source.
    """

    def __init__(self, data_type: str, file_path: str):
        self.data_type = data_type
        self.file_path = file_path


class Extractor(ABC):
    """
    Generic abstract class for data extractors.
    """

    def __init__(self, config: Optional[DataSourceConfig] = None):
        """
        Optionally takes a DataSourceConfig object for configuration.
        """
        self.config = config

    @abstractmethod
    def extract(self) -> Dict[str, Any]:
        """
        Abstract method to extract data.
        Returns a dictionary with extracted dataframes.
        """
        pass


class BaseDataExtractor(Extractor):
    """
    Extractor for a single data source.
    Can be subclassed to provide a data_source_name.
    """

    def __init__(self, spark, config: Optional[DataSourceConfig] = None):
        """
        Optionally takes a DataSourceConfig object for configuration.
        """
        super().__init__(config)
        self.spark = spark

    def extract(self) -> Dict[str, Any]:
        if not self.config:
            raise ValueError(
                "Missing data source configuration (DataSourceConfig object)"
            )

        data_type = self.config.data_type
        file_path = self.config.file_path
        logger.info(f"Extracting data from: type - {data_type}, path - {file_path}")

        data = get_data_source(
            spark=self.spark,
            data_type=data_type,
            file_path=file_path,
        )

        return data


class MultiSourceExtractor(BaseDataExtractor):
    """
    Extractor for multiple data sources.

    Can handle both related and unrelated data sources based on the 'is_related' flag.
    """

    def __init__(
        self,
        spark,
        configs: List[DataSourceConfig],
        data_source_names: Optional[List[str]] = None,
    ):
        """
        Takes a list of DataSourceConfig objects, optionally data source names, and a flag indicating
        if the data sources are related.

        Args:
            configs (List[DataSourceConfig]): List of configurations for data sources.
            data_source_names (Optional[List[str]]): Optional list of data source names.
        """
        super().__init__(spark)
        self.configs = configs
        if data_source_names and len(data_source_names) != len(configs):
            raise ValueError("Length of data_source_names must match length of configs")
        self.data_source_names = data_source_names or [
            f"data_source_{i}" for i in range(len(configs))
        ]
        self.extracted_data = {}

    def extract(self) -> Dict[str, Any]:
        if not self.configs:
            raise ValueError("Missing data source configurations")

        for i, config in enumerate(self.configs):
            data_name = self.data_source_names[i]

            data = get_data_source(
                spark=self.spark, data_type=config.data_type, file_path=config.file_path
            )

            self.extracted_data[data_name] = data

        return self.extracted_data
