import logging
import os
from datetime import datetime


#log file name 
log_file_name = f"{datetime.now().strftime('%m%d%Y__%H%M%S')}.log"

#create logging folder
log_directory = os.path.join(os.getcwd(), "logging_info")

#create logging folder if not already created
os.makedirs(log_directory, exist_ok=True)

# Logging file path 
log_file_path = os.path.join(log_directory, log_file_name)

# Log Config
logging.basicConfig(
    filename= log_file_path,
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

