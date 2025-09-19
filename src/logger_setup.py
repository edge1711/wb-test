import logging
import sys

def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('../app.log', encoding="utf-8")
        ],
        force=True
    )
    return logging.getLogger(name)
