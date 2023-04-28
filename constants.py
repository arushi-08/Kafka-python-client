import os

BOOTSTRAP_SERVER = os.environ.get('BOOTSTRAP_SERVER', "127.0.0.1:9093")
GROUP_ID = "group"
TOPIC = "page-views"
NUM_PARTITION = 3