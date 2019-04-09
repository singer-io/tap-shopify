#!/usr/bin/env python3
import json
import time
from prettytable import PrettyTable
from original_tap import ORIGINAL_TAP
from sidd_endpoint_tap import SIDD_ENDPOINT_TAP
from sidd_async_tap import SIDD_ASYNC_TAP

RUN_ORIGINAL_TAP = False
RUN_SIDD_ENDPOINT_TAP = True
RUN_SIDD_ASYNC_TAP = True


def main():
    with open('credentials.json') as f:
        credentials = json.load(f)

    with open('orders-schema.json') as f:
        stream = json.load(f)
    
    config = {
        'shop_name': credentials['shop_name'],
        'api_key': credentials['api_key'],
        'api_password': credentials['api_password'],
        'start_date': "2019-02-28 00:00:00",
        'end_date': "2019-03-29 23:59:59",
        'max_retries': 15,
        'results_per_page': 250,
        'stream_result_key': 'orders',
        'stream_endpoint': '/orders',
        'stream_id': stream['tap_stream_id'],
        'stream_schema': stream['schema'],
        'stream_key_props': stream['key_properties'],
        'stream_replication_key': stream['replication_key'],
        'stream_metadata': stream['metadata']
    }

    results = {}
    if RUN_ORIGINAL_TAP:
        rec_count, duration = ORIGINAL_TAP(config)
        results['original_tap'] = {'rec_count': rec_count, 'duration': duration}
    
    if RUN_SIDD_ENDPOINT_TAP:
        rec_count, duration = SIDD_ENDPOINT_TAP(config)
        results['sidd_endpoint_tap'] = {'rec_count': rec_count, 'duration': duration}
    
    if RUN_SIDD_ASYNC_TAP:
        rec_count, duration = SIDD_ASYNC_TAP(config)
        results['sidd_async_tap'] = {'rec_count': rec_count, 'duration': duration}
    
    for k,v in results.items():
        div = "-"*50
        info_msg = "{d}\n {name}\n{d}".format(name=k, d=div)
        info_msg += "\n  Num. Records: {r}".format(r=v['rec_count'])
        info_msg += "\n  Duration    : {r}".format(r=v['duration'])
        info_msg += "\n{d}".format(d=div)
        print(info_msg)
        


if __name__ == '__main__':
    main()