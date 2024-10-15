import logging
from datetime import datetime

class CustomLogger(logging.Logger):
    def _get_custom_timestamp(self):
        """Returns a custom timestamp in the format YYMMDDHHMMSSffffff."""
        return datetime.now().strftime("%y%m%d%H%M%S%f")

    def start(self, class_name, function_name, message):
        """Log a start message with INFO level."""
        custom_timestamp = self._get_custom_timestamp()
        self.info(f"[{class_name}][{function_name}] - START : {message}, {custom_timestamp}")

    def cwarning(self, class_name, function_name, message):
        """Log a warning message."""
        custom_timestamp = self._get_custom_timestamp()
        self.warning(f"[{class_name}][{function_name}] - WARNING : {message}, {custom_timestamp}")

    def error(self, class_name, function_name, message):
        """Log an error message."""
        custom_timestamp = self._get_custom_timestamp()
        super().error(f"[{class_name}][{function_name}] - ERROR : {message}, {custom_timestamp}")

    def success(self, class_name, function_name, message):
        """Log a success message with INFO level.""" 
        custom_timestamp = self._get_custom_timestamp()
        self.info(f"[{class_name}][{function_name}] - SUCCESS : {message}, {custom_timestamp}")

# Setup logger function
def setup_logger(level=logging.INFO):
    logging.setLoggerClass(CustomLogger)  # Set the logger class to CustomLogger
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(r"C:\Users\amank\Downloads\ft_data_codebase\appleHealthDataLogs.log"),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    return logger
