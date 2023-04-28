import pandas as pd
from google.cloud import storage
from sklearn.linear_model import LogisticRegression
import numpy as np
from sklearn.metrics import f1_socre, roc_curve, roc_auc_score
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D


pd.options.display.expand_frame_repr = False
pd.options.display.max_rows = 100

metadata = pd.read_csv('gcs://frsght/datasets_stacked/metadata.csv')
ACLED = pd.read_csv('gcs://frsght/acled_labels/ACLED_Labels_032723.csv')
transformer_preds = pd.read_csv('gcs://frsght/model_predictions/chad_preds.csv')
xgb_preds = pd.read_csv('gcs://frsght/model_predictions/diana_preds.csv')
iso_table = pd.read_html('https://www.worlddata.info/countrycodes.php')[0]

iso2 = pd.Series(iso_table['ISO 3166-1 alpha3'].values, index=iso_table['Fips 10']).to_dict()
iso3 = pd.Series(iso_table['ISO 3166-1 alpha2'].values, index=iso_table['Fips 10']).to_dict()

metadata = metadata.drop_duplicates()
transformer_preds = transformer_preds.drop_duplicates()
metadata['country_ym'] =  metadata['yearmonth'].astype(str) + '_' + metadata['country']

xgb_preds['country_ym'] = xgb_preds['target_window'].str[0:4]+xgb_preds['target_window'].str[5:]+"_"+xgb_preds['fips']
xgb_preds['xgb_correct'] = xgb_preds['lagged_spike'].astype(int) == xgb_preds['prediction']

transformer_preds['tf_correct'] = transformer_preds['Spike'] == transformer_preds['class_preds']

preds = transformer_preds.merge(metadata, how = 'left', on = 'country_ym')

preds = preds.merge(xgb_preds, on = 'country_ym', how = 'inner')

tf_metrics = transformer_preds.groupby('Code').mean()
tf_metrics['iso2'] = tf_metrics.index.map(iso2)
tf_metrics['iso3'] = tf_metrics.index.map(iso3)
tf_metrics = tf_metrics[['tf_correct', 'iso2', 'iso3']]
xgb_metrics = xgb_preds.groupby('fips').mean()
xgb_metrics['iso2'] = xgb_metrics.index.map(iso2)
xgb_metrics['iso3'] = xgb_metrics.index.map(iso3)
xgb_metrics = xgb_metrics[['xgb_correct', 'iso2', 'iso3']]



def render_spikes(country, data=ACLED):
    df = data.copy()
    df = df[df['Country'] == country]
    highlight_ranges = ((i - 0.5, i + 0.5) for i in df[df['Spike'] == True].index)
    plt.plot(df.index, df['Trend'], label='Trend', color='coral')
    plt.plot(df.index, df['Fatalities'], label='Fatalities', color='maroon')
    plt.xticks(df.index, df['Month'] + ' ' + df['Year'].astype(str))
    # Add vertical bars to highlight the specified index ranges
    for start, end in highlight_ranges:
        plt.axvspan(start, end, alpha=0.2, color='firebrick')
    # Add a legend and show the plot
    plt.legend()
    spike_patch = mpatches.Rectangle((0, 0), 1, 1, facecolor='firebrick', alpha=0.2)
    f_line = Line2D([],[],color = 'maroon', label = 'Fatalities')
    t_line = Line2D([],[],color='coral', label = 'Trend Slope')

    plt.legend([f_line, t_line, spike_patch], ['Fatalities','Trend Slope','Spike'])
    plt.title(f'Political Violence in {country}')
    plt.show()

def any_conflict(fips, df):
    _df = df.copy()
    _df = _df[_df['code'] == fips]
    violence = _df['Spike'].sum()
    if violence  > 1:
        violence = 1

    return violence

