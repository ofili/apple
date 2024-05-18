import os
from configparser import ConfigParser, NoSectionError, NoOptionError
from pydantic import Field
from pydantic_settings import BaseSettings

from core.logging.logger import logger


config_file_path = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "config.ini"
)


class Settings(BaseSettings):
    config: ConfigParser = Field(default_factory=ConfigParser)

    def __init__(self, config_file_path: str = config_file_path):
        super().__init__()
        
        # Handle potential errors during configuration file reading
        if not os.path.exists(config_file_path):
            raise FileNotFoundError(
                f"Spark configuration file '{config_file_path}' not found"
            )

        self.config.read(config_file_path)
    
    def get_config(self, section: str, option: str, default=None):
        try:
            return self.config.get(section, option, fallback=default)
        except (NoSectionError, NoOptionError):
            if default is not None:
                logger.info(f"Using default value for {section}.{option}: {default}")
                return default
            logger.error(f"Missing required configuration: {section}.{option}")
            raise
    

settings = Settings()