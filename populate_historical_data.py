from sklearn.datasets import load_iris
import boto3
import uuid
from decimal import Decimal
import numpy as np
import json
import os

with open('deploy_config.json', 'r') as f:
    deploy_config = json.load(f)
with open('task_config.json', 'r') as f:
    task_config = json.load(f)

task_workspace = os.path.join(deploy_config['workspace_path'], task_config['task_id'])
HISTORICAL_DATA_IDS_LOC = os.path.join(task_workspace, 'historical_data_ids.txt')


def populate_historical_data(sample_size=200):
    tbl = boto3.resource('dynamodb', region_name='us-east-2').Table('backtesting-data')
    X, y = load_iris(return_X_y=True)

    min_X = np.min(X, axis=0)
    max_X = np.max(X, axis=0)

    data = np.random.uniform(min_X, max_X, [sample_size, 4])
    ids = []

    with tbl.batch_writer() as writer:
        for i in range(sample_size):
            x = data[i]
            data_id = str(uuid.uuid1())
            writer.put_item(Item={
                'data_id': data_id,
                'data': {
                  'sepal_length': Decimal('%f' % x[0]),
                  'sepal_width': Decimal('%f' % x[1]),
                  'petal_length': Decimal('%f' % x[2]),
                  'petal_width': Decimal('%f' % x[3]),
                }
            })
            ids.append(data_id)

    with open(HISTORICAL_DATA_IDS_LOC, 'w') as f:
        f.write('\n'.join(ids))


def clean_up_db():
    with open(HISTORICAL_DATA_IDS_LOC, 'r') as f:
        ids = f.readlines()

    tbl = boto3.resource('dynamodb', region_name='us-east-2').Table('backtesting-data')
    with tbl.batch_writer() as batch:
        for i in ids:
            batch.delete_item(
                Key={
                    'data_id': i
                }
            )


def read():
    with open(HISTORICAL_DATA_IDS_LOC, 'r') as f:
        ids = f.readlines()
    tbl = boto3.resource('dynamodb', region_name='us-east-2').Table('backtesting-data')
    for i in ids:
        r = tbl.get_item(Key={'data_id': i.strip()})
        print(r['Item'])


# populate_historical_data(10000)
clean_up_db()
# read()
