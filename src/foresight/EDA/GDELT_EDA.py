import datetime
import pyarrow.parquet as pq
import pandas as pd
import os
from sklearn.linear_model import LinearRegression
import numpy as np

pd.options.display.expand_frame_repr = False
pd.options.display.max_rows = 100

GDELT_p = pd.read_parquet(os.path.normpath(os.environ['GDELT_Sample_Path']))
GDELT_r = pd.read_parquet(os.path.normpath(os.environ['GDELT_Raw_Path']))