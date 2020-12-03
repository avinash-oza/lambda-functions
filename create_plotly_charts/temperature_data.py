import io
import logging

import arrow
import pandas as pd
import plotly.express as px
import requests
from pandas.tseries.frequencies import Day

from telegram_bot.config_util import ConfigHelper

c = ConfigHelper()
pd.options.plotting.backend = "plotly"
logger = logging.getLogger(__name__)


def get_temperatures(locations='ALL'):
    if locations == 'ALL':
        locations = c.get('temperature', 'locations')

    dt_format = '%Y-%m-%d %I:%M:%S %p'
    current_time = arrow.now().strftime(dt_format)

    s = requests.Session()
    s.headers.update({'X-Api-Key': c.get('temperature', 'api_key')})
    url = c.get('temperature', 'url')

    resp_text = f"""Time: {current_time}\n"""
    for loc in locations:
        try:
            resp = s.get(fr'{url}/temperatures/{loc}/today?limit=1')
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            logger.exception(f"Error when getting {loc}")
            resp_text += f"{loc}: Exception on getting value\n"
        else:
            r = resp.json()
            logger.info(f"Response is {r}")
            if 'data' in r and r['data']:
                data = r['data'][0]
                value = float(data['value'])
                ts = arrow.get(data['timestamp']).to('America/New_York').strftime('%m/%d %I:%M:%S %p')
                resp_text += f"{loc}: {value:.2f}F -> {ts}\n"
            else:
                resp_text += f"{loc}: Could not get value\n"
    return resp_text


def get_temperatures_for_locations(sd, ed, locations):
    df_list = []
    for loc in locations:
        one_df = _get_temperatures_for_range(sd, ed, loc)
        df_list.append(one_df)

    return pd.concat(df_list)


def build_temperature_chart(df):
    fig = px.line(df, x='timestamp', y='value', line_group='location', color='location',
                  title='Temperature')
    buf = io.BytesIO()
    fig.write_image(buf, format='png', scale=0.75)
    buf.seek(0)

    return buf


def _get_temperatures_for_range(sd, ed, location):
    params = {
        'limit': 9999,
        'freq': 'D',
        'sd_str': sd.strftime('%Y-%m-%d'),
        'ed_str': ed.strftime('%Y-%m-%d') if not isinstance(ed, str) else ed,
    }

    result = []
    s = requests.Session()
    s.headers.update({'X-Api-Key': c.get('temperature', 'api_key')})
    url = c.get('temperature', 'url')

    resp = s.get(fr'{url}/temperatures/{location}/ts', params=params)
    r = resp.json()
    if 'data' in r and r['data']:
        result.extend(r['data'])

    df = pd.DataFrame.from_records(result, columns=['timestamp', 'value'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['value'] = df['value'].astype(float)
    df['location'] = location
    return df.sort_values('timestamp')


if __name__ == '__main__':
    ed = pd.Timestamp.today()

    sd = ed - Day(5)

    test_df = _get_temperatures_for_range(sd, ed, 'outdoor')
    test_df2 = _get_temperatures_for_range(sd, ed, 'garage')
    df_total = pd.concat([test_df, test_df2])

    data = build_temperature_chart(df_total)
    with open('test.png', 'wb') as f:
        f.write(data.read())
