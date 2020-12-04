import argparse
import io
import logging

import boto3
import pandas as pd
import plotly.express as px
import requests
from pandas.tseries.frequencies import Day

pd.options.plotting.backend = "plotly"
logger = logging.getLogger(__name__)


class TemperatureData:

    @staticmethod
    def build_temperature_chart(df, as_of_time):
        fig = px.line(df, x='timestamp', y='value', line_group='location', color='location',
                      title=f'Temperature as of {as_of_time.strftime("%Y-%m-%d %H:%M:%S%p")}')
        buf = io.BytesIO()
        fig.write_image(buf, format='png', scale=0.75)
        buf.seek(0)

        return buf

    @staticmethod
    def write_image_to_s3(bucket, file_path, img_buffer):
        s3 = boto3.client('s3')
        s3.upload_fileobj(img_buffer, bucket, file_path)

    @classmethod
    def get_temperatures(cls, api_key, api_url, sd, ed, locations):

        df_list = []
        for loc in locations:
            one_df = cls._get_temperatures_for_range(api_key,
                                                     api_url,
                                                     sd, ed, loc)
            df_list.append(one_df)

        df_total = pd.concat(df_list)

        return df_total

    @classmethod
    def _get_temperatures_for_range(cls, api_key, api_url, sd, ed, location):
        params = {
            'limit': 9999,
            'freq': '4h',
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

    df = TemperatureData.get_temperatures(args.lambda_api_key, args.lambda_url, sd, ed, args.locations)
    # df.to_pickle('sample_data.pkl')
    # df = pd.read_pickle('sample_data.pkl')

    chart_bytes = TemperatureData.build_temperature_chart(df, ed)

    TemperatureData.write_image_to_s3(args.bucket, args.file_path, chart_bytes)