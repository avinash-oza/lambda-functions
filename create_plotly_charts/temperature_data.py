import io
import logging

import arrow
import argparse
import pandas as pd
import plotly.express as px
import requests
from pandas.tseries.frequencies import Day

pd.options.plotting.backend = "plotly"
logger = logging.getLogger(__name__)


def build_temperature_chart(df):
    fig = px.line(df, x='timestamp', y='value', line_group='location', color='location',
                  title='Temperature')
    buf = io.BytesIO()
    fig.write_image(buf, format='png', scale=0.75)
    buf.seek(0)

    return buf


def _get_temperatures_for_range(api_key, api_url, sd, ed, location):
    params = {
        'limit': 9999,
        'freq': 'D',
        'sd_str': sd.strftime('%Y-%m-%d'),
        'ed_str': ed.strftime('%Y-%m-%d') if not isinstance(ed, str) else ed,
    }

    result = []
    s = requests.Session()
    s.headers.update({'X-Api-Key': api_key})

    resp = s.get(fr'{api_url}/temperatures/{location}/ts', params=params)
    r = resp.json()
    if 'data' in r and r['data']:
        result.extend(r['data'])

    df = pd.DataFrame.from_records(result, columns=['timestamp', 'value'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['value'] = df['value'].astype(float)
    df['location'] = location
    return df.sort_values('timestamp')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--lambda-api-key', type=str)
    parser.add_argument('--lambda-url', type=str)
    parser.add_argument('--bucket', type=str)
    parser.add_argument('--file-path', type=str)
    parser.add_argument('--locations', type=str, nargs='+')

    args = parser.parse_args()

    ed = pd.Timestamp.today()
    sd = ed - Day(7)

    df_list = []
    for loc in args.locations:
        one_df = _get_temperatures_for_range(args.lambda_api_key,
                                             args.lambda_url,
                                             sd, ed, loc)
        df_list.append(one_df)

    df_total = pd.concat(df_list)

    data = build_temperature_chart(df_total)
    with open('test.png', 'wb') as f:
        f.write(data.read())
