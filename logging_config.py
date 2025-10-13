
import logging
import logging.handlers
import os

def setup_logging(log_level='INFO'):
    """Configures logging for the application."""
    
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file_path = os.path.join(log_dir, 'scheduler.log')
    error_log_path = os.path.join(log_dir, 'errors.log')

    # Root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Console handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler
    file_handler = logging.handlers.RotatingFileHandler(
        log_file_path, maxBytes=10*1024*1024, backupCount=5
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Error file handler
    error_handler = logging.FileHandler(error_log_path)
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    error_handler.setFormatter(error_formatter)
    logger.addHandler(error_handler)
