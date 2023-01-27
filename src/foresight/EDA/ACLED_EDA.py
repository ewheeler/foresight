import pandas as pd
import os

pd.options.display.expand_frame_repr = False
pd.options.display.max_rows = 100

acled_path = os.path.normpath(r"..\..\..\..\Data\ACLED_012423_CS.csv")

#Note: ACLED Data contains some odd chars that throw errors.
#This is a working solution for EDA, but we may need to
#correct the CSV by hand for more stable results
ACLED = pd.read_csv(acled_path, engine='python', error_bad_lines = False)


ACLED['event_date'] = pd.to_datetime(ACLED['event_date'])

labels = ACLED.groupby(
    [
        ACLED.country,
        ACLED.event_date.dt.year.rename('year'),
        ACLED.event_date.dt.month.rename('month')
    ]
    )['fatalities'].sum().reset_index()