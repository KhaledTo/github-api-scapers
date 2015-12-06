import multiprocessing as mp
import requests
import json
import datetime
import time
import random
import os
import sys
import httplib

from settings import ACCESS_TOKEN

usage = """
Usage: get_user_details.py [user JSON filename] [output JSON filename] [last ID|optional]

By default, the script appends to the output file.
"""

con = None

def establish_connection():
	global con
	print "Establishing connection in process {0:d}...".format(os.getpid())
	con = httplib.HTTPSConnection('api.github.com',443)

def get_user_details(user_login,user_id):
    global con
    try:
        print "Getting details for {0!s} ({1:d})".format(user_login, user_id)
        if not con:
            establish_connection()
        try:
            con.request('GET','/users/{0!s}?access_token={1!s}'.format(user_login, ACCESS_TOKEN))
            response = con.getresponse()
        except:
            print "Connection error, recreating and retrying in 5 seconds..."
            time.sleep(5)
            con = None
            raise Exception("Connection failed!")
        if response.status == 404:
            print "User {0!s} does not exist...".format(user_login)
            return ""
        elif response.status != 200 and response.status != 403:
            print response.status,response.read()
            print "Error, waiting 10 seconds before retrying..."
            time.sleep(10)
            raise Exception("connection failed!")
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
                time.sleep(60)
            raise Exception("Request limit exceeded!")
        content = response.read()
        user_details = json.loads(content)
        return content
    except KeyboardInterrupt as e:
        return ""
    except requests.exceptions.RequestException as e:
        print "Exception occured:",str(e)
        raise e

if __name__ == '__main__':

        if len(sys.argv) < 3:
                print usage
                exit(-1)
        users_filename = sys.argv[1]
        output_filename = sys.argv[2]
        manager = mp.Manager()
        pool_size = 4
        pool = mp.Pool(pool_size)
        if len(sys.argv) >= 4:
                since_id = int(sys.argv[3])
        else:
                since_id = 0
        running_tasks = 0

        task_list = []
        if since_id > 10000:
            print "Fast-forwarding to user-id {0!s} in user file, this can take a while...".format(str(since_id))
        with open(users_filename,"rb") as users_file, \
                 open(output_filename,"ab") as output_file:
                try:
                    while True:
                                try:
                                        user = json.loads(users_file.readline())
                                except ValueError:
                                        print "Done"
                                        break
                                if user['id'] <= since_id:
                                        continue
                                while True:
                                        time.sleep(0.1)
                                        for task in task_list:
                                                if task.ready():
                                                        del task_list[task_list.index(task)]
                                                        if not task.successful():
                                                                print "Failed to get user details for {0!s}, retrying...".format(task.user['login'])
                                                                new_task = pool.apply_async(get_user_details,[task.user['login'],task.user['id']])
                                                                new_task.user = task.user
                                                                task_list.append(new_task)
                                                                break
                                                        result = task.get().strip()
                                                        if result:
                                                            output_file.write(result+"\n")
                                                            output_file.flush()
                                        if len(task_list) < pool_size:
                                                task = pool.apply_async(get_user_details,[user['login'],user['id']])
                                                task.user = user
                                                task_list.append(task)
                                                break
                except KeyboardInterrupt:
                    print "Quitting..."
                    exit(0) 
                finally:
                    print "When relaunching this script, use the following minimum ID: {0:d}".format((min([task.user['id'] for task in task_list])-1))
