import os
from google.cloud import storage
import numpy as np
import pandas as pd
import tensorflow.data as tfdata


pd.options.display.expand_frame_repr = False
pd.options.display.max_rows = 100

GCP_project = os.environ['GCP_project']

storage_client = storage.Client(project = GCP_project)

ACLED = pd.read_csv(os.path.normpath(os.environ['ACLED_Labels']))

codes = pd.read_html('https://www.worlddata.info/countrycodes.php')

codes = codes = dict(zip(codes['Country'], codes['Fips 10']))

#TODO fix FIPS code parsing for missing counties
ACLED['Code'] = ACLED['Country'].map(codes)

