import pandas as pd
import os
import numpy as np

pd.options.display.expand_frame_repr = False
pd.options.display.max_rows = 100

"""
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

"""

#humdata_path = os.path.normpath(os.environ['humdata_path'])

humdata_path = os.path.normpath(r'..\..\..\..\Data\ACLED_via_humdata_012623.xlsx')

ACLED = pd.read_excel(humdata_path, sheet_name='Data')

month_num = { 'January'  :   1
             ,'February' :   2
             ,'March'    :   3
             ,'April'    :   4
             ,'May'      :   5
             ,'June'     :   6
             ,'July'     :   7
             ,'August'   :   8
             ,'September':   9
             ,'October'  :   10
             ,'November' :   11
             ,'December' :   12 }

ACLED['Month_Num'] = ACLED['Month'].map(month_num)

#simple boolean: Did people die or not?
ACLED['Fatalities_Bool'] = ACLED['Fatalities']>1

#ACLED['More_Than_Average'] = ACLED['Fatalities']>
#ACLED['More_Than_Average_Since_2021'] = ACLED['Fatalities']>

def month_template (start_month, year, n_months):

    if type(start_month) == str:
        start_month = month_num[start_month]
    month_years = [(start_month, year)]
    month = start_month
    for i in range(n_months-1):
        if month <=1:
            month = 12
            year = year-1
        else:
            month = month-1

        month_years.append((month, year))

    df = pd.DataFrame(month_years)
    df.rename({0:'Month', 1:'Year'}, axis = 1, inplace=True)

    return df
