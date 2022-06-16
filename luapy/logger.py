import os


RUNNING_LOCAL = str(os.getenv("RUNNING_LOCAL", "0")) == "1"

if not RUNNING_LOCAL:
    import google.cloud.logging
    gcp_loggging_client = google.cloud.logging.Client()
    gcp_loggging_client.setup_logging()

import logging
import sys

# FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.getLogger('apscheduler').setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)
logger.debug('debugging...')
