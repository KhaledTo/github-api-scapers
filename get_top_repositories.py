import multiprocessing as mp
import requests
import json
import datetime
import time
import random
import os
import sys
import httplib
import gzip

from settings import ACCESS_TOKEN

usage = """
Usage: {0!s} [language] [output JSON filename]

By default, the script appends to the output file.
""".format(sys.argv[0])

headers = {
    'User-Agent': 'Github Scraper',
}

def establish_connection():
    print "Establishing connection in process {0:d}...".format(os.getpid())
    con = httplib.HTTPSConnection('api.github.com',443)
    return con

def smart_open(filename,*args,**kwargs):
    if filename.endswith(".gz"):
        open_func = gzip.open
    else:
        open_func = open
    return open_func(filename,*args,**kwargs)

def get_language_repos(language,page = 1):
    try:
        all_language_repositories = []
        while True:
            print "Getting top repositories for {0!s}".format((language))
            con = establish_connection()
            try:
                con.request('GET','/search/repositories?q=language:{0!s}&sort=stars&order=desc&access_token={1!s}&page={2:d}'.format(language, ACCESS_TOKEN, page),headers = headers)
                response = con.getresponse()
            except:
                print "Connection error"
                break
            if response.status == 404:
                break
            elif response.status != 200 and response.status != 403:
                print "Invalid response..."
                break
            remaining_requests = int(response.getheader('x-ratelimit-remaining'))
            reset_time = datetime.datetime.fromtimestamp(int(response.getheader('x-ratelimit-reset')))
            print "{0:d} requests remaining...".format((remaining_requests))
            if remaining_requests == 0:
                print "Allowed requests depleted, waiting..."
                while True:
                    if reset_time < datetime.datetime.now():
                        print "Continuing!"
                        break
                    waiting_time_seconds = (reset_time-datetime.datetime.now()).total_seconds()
                    waiting_time_minutes = int(waiting_time_seconds/60)
                    waiting_time_seconds_remainder = int(waiting_time_seconds) % 60
                    print "{0:d} minutes and {1:d} seconds to go".format(waiting_time_minutes, waiting_time_seconds_remainder)
                    time.sleep(10)
                break
            content = response.read()
            language_repositories = json.loads(content)
            if 'items' in language_repositories:
                all_language_repositories.extend(language_repositories['items'])
            if not len(language_repositories):
                break
            print "Next page..."
            page+=1
        print "Got {0:d} repositories".format(len(all_language_repositories))
    except KeyboardInterrupt as e:
        print "Keyboard interrupt..."
    except requests.exceptions.RequestException as e:
        print "Exception occured:",str(e)
    return "\n".join([json.dumps(repo).strip() for repo in all_language_repositories]),page

if __name__ == '__main__':

        if len(sys.argv) < 3:
                print usage
                exit(-1)

        language = sys.argv[1]
        output_filename = sys.argv[2]

        running_tasks = 0

        task_list = []
        current_page = 1
        with smart_open(output_filename,"wb") as output_file:
            while True:
                try:
                    repos,last_page = get_language_repos(language,current_page)
                    if not repos.strip():
                        break
                    output_file.write(repos)
                    current_page = last_page
                except KeyboardInterrupt:
                    break 
