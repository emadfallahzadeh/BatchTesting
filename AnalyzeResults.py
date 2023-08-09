import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import configparser
import psycopg2.extras
import seaborn as sns
import datetime
import sys

config_parser = configparser.RawConfigParser()
config_file_path = r'config.conf'
config_parser.read(config_file_path)
batch_size = int(config_parser.get('General', 'batch_size'))
db_name = config_parser.get('General', 'db_name')
cpu_count = int(config_parser.get('General', 'cpu_count'))

con = psycopg2.connect(database=db_name, user="postgres", password="secret", host="localhost", port="5432")
cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)

def get_unit_divider(unit):
    if unit == 'hour':
        unit_divider = 3600
    elif unit == 'day':
        unit_divider = 86400
    elif unit == 'second':
        unit_divider = 1
    return unit_divider

def get_average_feedback_time(unit='hour'):
    unit_divider = get_unit_divider(unit)
    algorthms_types = ['testall', 'constantbatching_2', 'constantbatching_4', 'batchall', 'testcasebatching']
    for algorithm in algorthms_types:
        if algorithm.startswith('constantbatching'):
            algorithm_feedback = 'constantbatching' + '_' + str(cpu_count) + 'cpu_batch' + algorithm[-1] + '_feedback'
        else:
            algorithm_feedback = algorithm + '_' + str(cpu_count) + 'cpu_feedback'
        query = 'select avg(a.end_time - a.commit_time) ' \
                'from {} a'.format(algorithm_feedback)
        cur.execute(query)
        average_feedback_time = cur.fetchone()[0]
        print("average feedback time for " + algorithm_feedback + ":" + str(average_feedback_time.total_seconds()/unit_divider) + " in hours")
        query = 'select avg(a.end_time - a.start_time) ' \
                'from {} a'.format(algorithm_feedback)
        cur.execute(query)
        average_cpu_time = cur.fetchone()[0]
        print("average cpu time for " + algorithm_feedback + ":" + str(average_cpu_time.total_seconds()/unit_divider * cpu_count) + " in hours")

get_average_feedback_time()
