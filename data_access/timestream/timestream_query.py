# -*- coding:utf-8 -*-
# (c) Dileep Jayamal. All Rights Reserved
#  Jayamal
from typing import List, Dict
import numpy as np
import pandas as pd
import datetime
import boto3
pd.options.mode.chained_assignment = None

"""
Load timestream data as a pandas dataframe 
"""


def phrase_to_df(data: List[Dict], column_info: List[Dict]) -> pd.DataFrame:
    dataset = {}
    for column in column_info:
        dataset[column['Name']] = []
    for row in data:
        row = row['Data']
        for elm, column in zip(row, column_info):
            scalar_value = elm.get('ScalarValue')
            dataset[column['Name']].append(scalar_value)
    df = pd.DataFrame(dataset)
    for column in column_info:
        if column['Type']['ScalarType'] == "TIMESTAMP":
            df[column['Name']] = df[column['Name']].apply(lambda x: datetime.datetime.fromisoformat(x.split(".")[0]))
        elif column['Type']['ScalarType'] == "DOUBLE":
            df[column['Name']] = df[column['Name']].apply(lambda x: float(x) if x is not None else np.NaN)
        elif column['Type']['ScalarType'] == "BOOLEAN":
            df[column['Name']] = df[column['Name']].apply(lambda x: 'true' in x if x is not None else np.NaN)
        elif column['Type']['ScalarType'] == "VARCHAR":
            df[column['Name']] = df[column['Name']].apply(lambda x: str(x) if x is not None else np.NaN)
        elif column['Type']['ScalarType'] == "BIGINT":
            df[column['Name']] = df[column['Name']].apply(lambda x: int(x) if x is not None else np.NaN)

    return df


def load_as_df(client: boto3.client, query: str) -> pd.DataFrame:
    """
    :param client: boto3.client - timestream client (boto3.client('timestream-query'))
    :param query: str
    :return: pd.DataFrame
    """
    rs = client.query(QueryString=query)
    data = rs['Rows']
    column_info = rs['ColumnInfo']
    next_token = rs.get('NextToken')
    while next_token:
        rs = client.query(QueryString=query, NextToken=next_token)
        data.append(rs['Rows'])
        next_token = rs.get('NextToken')

    return phrase_to_df(data, column_info)

