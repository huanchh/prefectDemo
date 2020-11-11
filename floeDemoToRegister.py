import requests
import json
from prefect import task, Flow
from contextlib import closing
import sqlite3
import datetime

from collections import namedtuple
from prefect.tasks.database.sqlite import SQLiteScript
from prefect.schedules import IntervalSchedule
from prefect.engine import signals
# there is a result handler for Azure, S3
# the following will just write to local file
from prefect.engine.result_handlers import LocalResultHandler


# signature has to be like so
def alert_failed(obj, old_state, new_state):
    if new_state.is_failed():
        print("failed")


## extract, with cache so it will not hit consumer finance.gov for one day
## stores in memory
# @task(cache_for=datetime.timedelta(days=1), state_handlers=[alert_failed], result_handler=LocalResultHandler())
@task(state_handlers=[alert_failed], result_handler=LocalResultHandler())
def get_complaint_data():
    r = requests.get("https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/",
                     params={'size': 10})
    response_json = json.loads(r.text)
    print("I actually requested this time ")
    return response_json['hits']['hits']


## transform
@task
def parse_complaint_data(raw, state_handlers=[alert_failed]):
    # raise

    complaints = []
    Complaint = namedtuple('Complaint', ['date_received', 'state', 'product', 'company', 'complaint_what_happened'])
    for row in raw:
        source = row.get('_source')
        this_complaint = Complaint(
            date_received=source.get('date_received'),
            state=source.get('state'),
            product=source.get('product'),
            company=source.get('company'),
            complaint_what_happened=source.get('complaint_what_happened')
        )
        complaints.append(this_complaint)
    return complaints


## load
@task
def store_complaints(parsed, state_handlers=[alert_failed]):
    # create_script = 'CREATE TABLE IF NOT EXISTS complaint (timestamp TEXT, state TEXT, product TEXT, company TEXT, complaint_what_happened TEXT)'
    insert_cmd = "INSERT INTO complaint VALUES (?, ?, ?, ?, ?)"

    with closing(sqlite3.connect("cfpbcomplaints.db")) as conn:
        with closing(conn.cursor()) as cursor:
            # cursor.executescript(create_script)
            cursor.executemany(insert_cmd, parsed)
            conn.commit()
    pass


## Example using built in task as supposed to writing our own
create_table = SQLiteScript(
    db='cfpbcomplaints.db',
    script='CREATE TABLE IF NOT EXISTS complaint (timestamp TEXT, state TEXT, product TEXT, company TEXT, complaint_what_happened TEXT)'
)

## prefect lowest is one minute
schedule = IntervalSchedule(interval=datetime.timedelta(minutes=1))

#with Flow("my etl flow", schedule) as f:
with Flow("my etl flow") as f:
    # with Flow("my etl flow") as f:
    db_table = create_table()
    raw = get_complaint_data()
    parsed = parse_complaint_data(raw)
    populated_table = store_complaints(parsed)

    # set pre-requisite
    populated_table.set_upstream(db_table)

f.register(project_name='hello World')
