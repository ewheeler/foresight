import datetime

import pandas as pd
import os
from sklearn.linear_model import LinearRegression
import numpy as np

pd.options.display.expand_frame_repr = False
pd.options.display.max_rows = 100

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

ACLED['Date'] = pd.to_datetime(ACLED['Month'] + ' ' + ACLED['Year'].astype(str))

#simple boolean: Did people die or not?
ACLED['Fatalities_Bool'] = ACLED['Fatalities']>1

#ACLED['More_Than_Average'] = ACLED['Fatalities']>
#ACLED['More_Than_Average_Since_2021'] = ACLED['Fatalities']>


def get_months (start_month, year, n_months):
    """
    Returns a DF with years/months for the time period begining
    1 month before start_month and ending n months before start_month
    """

    if type(start_month) == str:
        start_month = month_num[start_month]

    month_years = []
    month = start_month

    for i in range(n_months):
        if month <= 1:
            month = 12
            year = year - 1
        else:
            month = month - 1

        month_years.append((year, month))

    start_date = month_years[0]
    end_date = month_years[-1]

    date_range = (
         datetime.datetime(start_date[0], start_date[1], 1)
        ,datetime.datetime(end_date[0], end_date[1], 1)
    )

    return date_range

def create_base_df(start_month, start_year, country, n_months):
    """
    Creates a base df of n months from the start date for a given country
    This can be used for further trend analysis
    """
    daterange = get_months(start_month,start_year, n_months)

    df = ACLED[
             (ACLED['Country'].values == country)
           & (ACLED['Date'].values >= np.datetime64(daterange[1]))
           & (ACLED['Date'].values <= np.datetime64(daterange[0]))
    ].reset_index()
    return df


def fatalities_previous_month(start_month, start_year, country):
    base_df = create_base_df(start_month, start_year, country, 1)
    return base_df['Fatalities'].iloc[0]


def n_month_mean(start_month, start_year, country, n_months):
    base_df = create_base_df(start_month, start_year, country, n_months)
    return base_df['Fatalities'].mean()


def n_month_trend(start_month, start_year, country, n_months):
    base_df = create_base_df(start_month, start_year, country, n_months)
    x = base_df.index.values.reshape(-1,1)
    y = base_df['Fatalities']
    model = LinearRegression()
    model.fit(x,y)
    slope = model.coef_
    return slope