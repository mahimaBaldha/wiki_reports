import datetime
import json
import time

import schedule
from sseclient import SSEClient as EventSource


def api_call():
    url = 'https://stream.wikimedia.org/v2/stream/revision-create'
    domains_report, user_report = [], []
    current_time = datetime.datetime.now()
    one_minute = datetime.timedelta(hours=5, minutes=30, seconds=4)
    previous_minute = current_time - one_minute
    for event in EventSource(url):
        try:
            change = json.loads(event.data)
            date_time_obj = datetime.datetime.strptime(change['meta']['dt'], "%Y-%m-%dT%H:%M:%SZ").strftime(
                "%Y-%m-%d %H:%M")
            meta_data = change['meta']
            if date_time_obj <= previous_minute.strftime("%Y-%m-%d %H:%M"):
                # DOMAIN
                domain = meta_data['domain']
                same_domain = list(filter(lambda person: person['domain'] == domain, domains_report))
                if not same_domain:
                    report_1 = {'domain': domain, 'title': {change['page_title']}}
                    domains_report.append(report_1)
                else:
                    same_domain[0]["title"].add(change['page_title'])

                # USER
                if domain == "en.wikipedia.org":
                    if "performer" in change and "user_edit_count" in change:
                        wiki_user = list(
                            filter(lambda person: person['user'] == change["performer"]["user_text"], user_report))
                        if wiki_user:
                            wiki_user[0]["user_edit_count"] = change["performer"]["user_edit_count"] \
                                if change["performer"]["user_edit_count"] > wiki_user[0]["user_edit_count"] \
                                else wiki_user[0]["user_edit_count"]
                        else:
                            if not change["performer"]["user_is_bot"]:
                                record = {"user": change["performer"]["user_text"],
                                          "user_edit_count": change["performer"]["user_edit_count"]}
                                user_report.append(record)
            else:
                break
        except ValueError:
            pass
    print("Total number of Wikipedia Domains Updated: {}".format(len(domains_report)))
    # for data in domains_report:
    for data in sorted(domains_report, key=lambda data: len(data['title']), reverse=True):
        print("{} : {} Pages Updated".format(data['domain'], len(data['title'])))
    print("******************************************************************************************")
    print("Users who made changes to en.wikipedia.org : {}".format(len(user_report)))
    for data in sorted(user_report, key=lambda data: data['user_edit_count'], reverse=True):
        print("{} : {}".format(data['user'], data['user_edit_count']))
    pass


schedule.every(1).minutes.do(api_call)

while True:
    schedule.run_pending()
    time.sleep(1)
