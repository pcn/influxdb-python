# -*- coding: utf-8 -*-
"""
unit tests
"""
import json

import time
import requests
from nose.tools import raises
from mock import patch

from influxdb import InfluxDBClient
import influxdb.graphite as graphite

USER='testuser'
PASS='testpassword'
PORT=8086
DBNAME='graphite'
HOST='10.255.0.2'
TEST_METRIC_NAME='peter.test.metric'

retention = "60m:1y"
fixed_ret = "60m_1y"
retention_tags={"gr-ret":fixed_ret}

def test_single_metric_json():
    "No connection, just tests json"
    now = time.time()
    db_name = "graphite"
    test_metric = (TEST_METRIC_NAME, now, now)

    influxdb_metric = graphite.graphite_metric_to_influxdb(test_metric, retention_tags)

    expected_result = {'columns': ['time', 'value', 'gr-ret'],
                       'name': TEST_METRIC_NAME,
                       'points': [[int(now * 1000), now, '60m_1y']]} # * 1000 for ms since the epoch
    assert (set(influxdb_metric['columns']) == set(expected_result['columns']))
    assert (set(influxdb_metric['points'][0]) == set(expected_result['points'][0]))
    assert (set(influxdb_metric['name']) == set(expected_result['name']))
    assert (influxdb_metric.keys() == expected_result.keys())

def test_send_to_influxdb():
    conn = InfluxDBClient(HOST, PORT, USER, PASS, DBNAME)
    now = time.time()
    db_name = "graphite"
    test_metric = (TEST_METRIC_NAME, now, now)
    influxdb_metric = graphite.graphite_metric_to_influxdb(test_metric, retention_tags)

    print (influxdb_metric)
    url = "{0}/db/{1}/series?u={2}&p={3}&time_precision={4}".format(conn._baseurl,
                                                                    conn._database,
                                                                    conn._username,
                                                                    conn._password,
                                                                    'm')
    print url
    response = requests.post(url, data=json.dumps([influxdb_metric]))
    return response
    print dir(response)
    print response
    conn.write_points_with_precision(influxdb_metric, 'm')

def test_example_send_to_influxdb():
    conn = InfluxDBClient(HOST, PORT, USER, PASS, DBNAME)
    conn.switch_db(DBNAME)
    now = time.time()
    db_name = "graphite"
    test_metric = (TEST_METRIC_NAME, now, now)
    influxdb_metric = [ { "name": "response_times",
                          "columns": ["time", "value"],
                          "points": [ [1382819388, 234.3],
                                      [1382819389, 120.1],
                                      [1382819380, 340.9]
                                  ] } ]

    print (influxdb_metric)
    url = "{0}/db/{1}/series?u={2}&p={3}&time_precision={4}".format(conn._baseurl,
                                                                    conn._database,
                                                                    conn._username,
                                                                    conn._password,
                                                                    'm')
    print url
    response = requests.post(url, data=json.dumps([influxdb_metric]))
    return response


def test_metric_list_json():
    pass
