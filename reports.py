import datetime
import json
import time

import schedule
from sseclient import SSEClient as EventSource

STREAM_URL = "https://stream.wikimedia.org/v2/stream/revision-create"


def generate_reports(previous_minute):
    domains_report, user_report = [], []
    for event in EventSource(STREAM_URL):
        try:
            response = json.loads(event.data)
            date_time_obj = datetime.datetime.strptime(response['meta']['dt'], "%Y-%m-%dT%H:%M:%SZ").strftime(
                "%Y-%m-%d %H:%M")
            meta_data = response['meta']
            if date_time_obj <= previous_minute.strftime("%Y-%m-%d %H:%M"):
                # DOMAIN REPORT
                domain = meta_data['domain']
                same_domain = list(filter(lambda report_data: report_data['domain'] == domain, domains_report))
                if not same_domain:
                    report_1 = {'domain': domain, 'title': {response['page_title']}}
                    domains_report.append(report_1)
                else:
                    same_domain[0]["title"].add(response['page_title'])

                # USER REPORT
                if domain == "en.wikipedia.org":
                    if "performer" in response and "user_edit_count" in response:
                        wiki_user = list(
                            filter(lambda report_data: report_data['user'] == response["performer"]["user_text"], user_report))
                        if wiki_user:
                            wiki_user[0]["user_edit_count"] = response["performer"]["user_edit_count"] \
                                if response["performer"]["user_edit_count"] > wiki_user[0]["user_edit_count"] \
                                else wiki_user[0]["user_edit_count"]
                        else:
                            if not response["performer"]["user_is_bot"]:
                                record = {"user": response["performer"]["user_text"],
                                          "user_edit_count": response["performer"]["user_edit_count"]}
                                user_report.append(record)
            else:
                break
        except ValueError:
            pass
    print("Total number of Wikipedia Domains Updated: {}".format(len(domains_report)))
    # for data in domains_report:
    for data in sorted(domains_report, key=lambda data: len(data['title']), reverse=True):
        print("{} : {} Pages Updated".format(data['domain'], len(data['title'])))
    print("Users who made responses to en.wikipedia.org : {}".format(len(user_report)))
    for data in sorted(user_report, key=lambda data: data['user_edit_count'], reverse=True):
        print("{} : {}".format(data['user'], data['user_edit_count']))
    pass


def task():
    current_time = datetime.datetime.now()
    # 5 hours and 30 minutes delay as api does not return current minute data. instead its time
    # is lesser than current time 5 hours and 30 minutes
    one_minute = datetime.timedelta(hours=5, minutes=30, seconds=4)
    previous_minute = current_time - one_minute
    # this will ensure that only previous one minute data has to be considered
    generate_reports(previous_minute)
    pass


def bonus_task():
    current_time = datetime.datetime.now()
    five_minute = datetime.timedelta(hours=5, minutes=35, seconds=4)
    previous_minute = current_time - five_minute
    # this will ensure that previous 5 minute data has to be considered
    generate_reports(previous_minute)
    pass


# report would be generated every minute
schedule.every(1).minutes.do(task)
schedule.every(1).minutes.do(bonus_task)

while True:
    schedule.run_pending()
    time.sleep(1)
