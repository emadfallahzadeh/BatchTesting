import psycopg2
import psycopg2.extras
import configparser
from datetime import datetime, timedelta
from collections import deque

config_parser = configparser.RawConfigParser()
config_file_path = r'config.conf'
config_parser.read(config_file_path)
db_name = config_parser.get('General', 'db_name')
cpu_count = int(config_parser.get('General', 'cpu_count'))
algorithm_type = 'testcasebatching_' + str(cpu_count) + 'cpu'
max_batch_size = int(config_parser.get('General', 'max_batch_size'))

con = psycopg2.connect(database=db_name, user="postgres", password="secret", host="localhost", port="5432")
cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)


def create_tables():
    cur.execute("drop table if exists {}".format(algorithm_type))
    cur.execute("create table {}("
                "build text,"
                "test_name text,"
                "verdict boolean,"
                "run_order int,"
                "run_time interval,"
                "main_run_order int)".format(algorithm_type))
    con.commit()
    cur.execute("drop table if exists {}_feedback".format(algorithm_type))
    cur.execute("create table {}_feedback("
                "build text,"
                "commit_time interval,"
                "start_time interval,"
                "end_time interval)".format(algorithm_type))
    con.commit()


def get_builds():
    # getting from builds table
    cur.execute(
        "select build, min(start_time) start_time from tests_unexpected group by build, start_time order by min(run_order) asc")
    builds = cur.fetchall()
    build_count = cur.rowcount
    return builds, build_count


def get_running_builds(build_counter, builds, start_time, run_time):
    batch_size = 1
    while (build_counter + batch_size) < len(builds) and \
            batch_size <= max_batch_size and \
            (builds[build_counter + batch_size]['start_time'] - start_time) <= run_time:  # next build has arrived
        batch_size += 1

    if batch_size == 1:  # subsequent builds are not available => only we should wait for the first one
        run_time = builds[build_counter]['start_time'] - start_time

    running_builds = builds[build_counter:build_counter + batch_size]
    build_counter += batch_size
    return build_counter, running_builds, run_time


def get_select_query():
    select_query = "select build, test_name, verdict, run_order, execution_time from tests_unexpected where build = '{}' order by run_order"
    return select_query


def get_running_tests(running_builds, builds_run_time_start, run_time):
    running_tests = deque()
    select_query = get_select_query()
    for build in running_builds:
        build_id = build['build']
        builds_run_time_start[build_id] = run_time
        filled_select_query = select_query.format(build_id)
        cur.execute(filled_select_query)
        new_fetched_tests = cur.fetchall()
        running_tests.extend(new_fetched_tests)
        running_tests.extend(["end_" + build_id])
    return running_tests, builds_run_time_start


class test_information:
    def __init__(self, build, name, verdict, main_run_order, execution_time):
        self.build = build
        self.name = name
        self.verdict = verdict
        self.main_run_order = main_run_order
        self.execution_time = execution_time


def get_test_info(test):
    test_build = test[0]
    test_name = test[1]
    test_verdict = test[2]
    test_main_run_order = test[3]
    test_execution_time = test[4]
    test_info = test_information(test_build, test_name, test_verdict, test_main_run_order, test_execution_time)
    return test_info


def add_fail_test_build(fail_test_builds, test_info):
    if test_info.name in fail_test_builds:
        fail_test_builds[test_info.name].append(test_info)
    else:
        fail_test_builds[test_info.name] = [test_info]


def update_run_order_time(run_order, run_time, test_info, cpu_count, cpu_time):
    run_order += 1
    try:
        run_time += test_info.execution_time / cpu_count
        cpu_time += test_info.execution_time / cpu_count
    except:
        run_time += timedelta(seconds=test_info.execution_time) / cpu_count
        cpu_time += timedelta(seconds=test_info.execution_time) / cpu_count
    return run_order, run_time, cpu_time


def add_available_builds_tests(build_counter, builds, start_time, run_time, running_builds, builds_run_time_start,
                               running_tests):
    batch_size = 1
    while (build_counter + batch_size) < len(builds) and \
            len(running_builds) + batch_size <= max_batch_size and\
            batch_size <= max_batch_size and \
            (builds[build_counter + batch_size]['start_time'] - start_time) <= run_time:  # next build has arrived
        batch_size += 1
    if batch_size == 1:  # subsequent builds are not available
        return build_counter, running_tests
    extending_builds = builds[build_counter:build_counter + batch_size]
    running_builds.extend(extending_builds)
    build_counter += batch_size

    select_query = get_select_query()
    for build in extending_builds:
        build_id = build['build']
        builds_run_time_start[build_id] = run_time
        filled_select_query = select_query.format(build_id)
        cur.execute(filled_select_query)
        new_fetched_tests = cur.fetchall()
        running_tests.extend(new_fetched_tests)
        running_tests.extend(["end_" + build_id])
    return build_counter, running_tests


def insert_runorder(algorithm_type, test_info, run_order, run_time):
    insert_run_order = "insert into {} (build, test_name, verdict, run_order, run_time, main_run_order)" \
                       " values(%(build)s, %(test_name)s, %(verdict)s, %(run_order)s, %(run_time)s, %(main_run_order)s)".format(
        algorithm_type)
    cur.execute(insert_run_order,
                {'build': test_info.build, 'test_name': test_info.name, 'verdict': test_info.verdict,
                 'run_order': run_order, 'run_time': run_time, 'main_run_order': test_info.main_run_order})
    # notice: commit outside


def find_store_culprit(fail_tests, running_builds, run_order, run_time, cpu_count, cpu_time):
    for fail_test_name in fail_tests:
        test_info = fail_tests[fail_test_name][0]
        for build in running_builds:
            run_order, run_time, cpu_time = update_run_order_time(run_order, run_time, test_info, cpu_count, cpu_time)
            for test_info in fail_tests[fail_test_name]:
                if build['build'] == test_info.build:
                    insert_runorder(algorithm_type, test_info, run_order, run_time)
                    con.commit()
    return run_order, run_time, cpu_time


def insert_build_feedback(build, commit_time, start_time, end_time):
    insert_feedback = "insert into {}_feedback (build, commit_time, start_time, end_time)" \
                      " values(%(build)s, %(commit_time)s, %(start_time)s, %(end_time)s)".format(algorithm_type)
    cur.execute(insert_feedback,
                {'build': build, 'commit_time': commit_time, 'start_time': start_time, 'end_time': end_time})
    con.commit()


def check_build_end(test):
    return isinstance(test, str) and test.startswith('end_')


def has_run_for_build(batch, test_name, build_id):
    if test_name in batch and build_id in batch[test_name]:
        return True
    else:
        return False


def add_to_batch(batch, test_name, running_builds):
    for build in running_builds:
        if test_name in batch:
            batch[test_name].add(build['build'])  # {test1: (b1, b2, ...), test2: (b2, b3)}
        else:
            batch[test_name] = {build['build']}
    return batch


def find_build_commit_time(running_builds, finished_build, start_time):
    for build in running_builds:
        if build['build'] == finished_build:
            return build['start_time'] - start_time


def remove_finished_build(running_builds, finished_build, batch):
    for test_name in batch:
        if finished_build in batch[test_name]:
            batch[test_name].remove(finished_build)

    for build in running_builds:
        if build['build'] == finished_build:
            running_builds.remove(build)
            return running_builds

def process_batch(build_counter, builds, start_time, fail_tests, running_builds, run_order, run_time, cpu_count, batch, builds_run_time_start, cpu_time):
    for build in running_builds:
        running_tests, builds_run_time_start = get_running_tests([build], builds_run_time_start, run_time)
        while running_tests:
            test = running_tests.popleft()
            if check_build_end(test):
                run_order, run_time, cpu_time = find_store_culprit(fail_tests, running_builds, run_order, run_time, cpu_count, cpu_time)
                finished_build = test[4:]
                build_commit_time = find_build_commit_time(running_builds, finished_build, start_time)
                insert_build_feedback(finished_build, build_commit_time, builds_run_time_start[finished_build], run_time)
                running_builds = remove_finished_build(running_builds, finished_build, batch)
                continue
            test_info = get_test_info(test)
            if test_info.verdict == False:
                add_fail_test_build(fail_tests, test_info)
            has_run_for_build_flag = has_run_for_build(batch, test_info.name, test_info.build)
            if not has_run_for_build_flag or test_info.verdict == False:
                run_order, run_time, cpu_time = update_run_order_time(run_order, run_time, test_info, cpu_count, cpu_time)

            # if new add to batch union
            if not has_run_for_build_flag:
                batch = add_to_batch(batch, test_info.name, running_builds)
            build_counter, running_tests = add_available_builds_tests(build_counter, builds, start_time, run_time,
                                                                      running_builds, builds_run_time_start, running_tests)
    return build_counter, run_time, cpu_time


create_tables()
builds, build_count = get_builds()
start_time = builds[0]['start_time']
run_order = 0
run_time = timedelta()
cpu_time = timedelta()
build_counter = 0

while build_counter < len(builds):
    build_counter, running_builds, run_time = get_running_builds(build_counter, builds, start_time, run_time)
    builds_run_time_start = {}
    batch = {}
    fail_tests = {}
    build_counter, run_time, cpu_time = process_batch(build_counter, builds, start_time, fail_tests, running_builds,
                                                      run_order, run_time, cpu_count, batch, builds_run_time_start, cpu_time)

average_cpu_time = cpu_time.total_seconds() / 3600 * cpu_count / build_count
print("average cpu_time for {} algorithm with {} cpu: {} hours".format(algorithm_type, cpu_count, average_cpu_time))
con.close()
