import logging
import ecs_logging



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add a handler to the logger
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = ecs_logging.StdlibFormatter()
handler.setFormatter(formatter)
logger.addHandler(handler)
