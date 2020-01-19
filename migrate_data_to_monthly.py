import boto3
import arrow
import time
import itertools
import pandas as pd

# copies data from daily to monthly partitions


if __name__ == '__main__':
    start_date = '2018-01-01'
    end_date = '2018-12-31'

    locations = ['OUTDOOR', 'GARAGE', 'CASE']

    ddb = boto3.client('dynamodb')

    for dt, one_location in itertools.product(pd.date_range(start_date, end_date), locations):
        dt_str = dt.strftime('%Y%m%d')
        one_res = ddb.query(TableName='dataTable', KeyConditionExpression="key_name = :k_name",
                            ExpressionAttributeValues={':k_name': {'S': 'temperature+{}+{}'.format(one_location, dt_str)}},
                            ScanIndexForward=False)
        if not one_res['Count']:
            continue

        ops_list = []
        for r in one_res['Items']:
            #TODO: DELETE
            # ops_list.append({'DeleteRequest': dict(Key={'key_name': {'S': f'temperature+{one_location}+{dt_str}'}, 'timestamp': {'S': r['timestamp']['S']}})})

            # TODO: INSERT
            new_res = r.copy()
            new_dt = dt.strftime('%Y%m')
            new_res['key_name'] = {'S' :f'temperature+{one_location}+{new_dt}' }
            ops_list.append({'PutRequest': {'Item': new_res}})
            if len(ops_list) == 20:
                print(f"Writing batch {one_location}+{dt_str}")
                ddb.batch_write_item(RequestItems={'dataTable': ops_list})
                time.sleep(1)
                ops_list = []

        if len(ops_list):
            print(f"Writing batch {one_location}+{dt_str}")
            ddb.batch_write_item(RequestItems={'dataTable': ops_list})
            time.sleep(1)
