import requests
import json
import datetime
import time
import sys
import math

from settings import ACCESS_TOKEN

usage = """Retrieves a list of all Github users using the Github API.

Usage: get_all_users.py [output JSON filename] [since ID|optional]
"""

if __name__ == '__main__':

	if len(sys.argv) < 2:
		print usage
		exit(-1)
	output_filename = sys.argv[1]
	params = {'access_token' : ACCESS_TOKEN}
	if len(sys.argv) >= 3:
		params['since'] = sys.argv[2]
	try:
		with open(output_filename,"ab") as output_file:
			while True:
				r = requests.get('https://api.github.com/users',params=params)
				remaining_requests = int(r.headers['x-ratelimit-remaining'])
				reset_time = datetime.datetime.fromtimestamp(int(r.headers['x-ratelimit-reset']))
				waiting_time = (reset_time-datetime.datetime.now()).total_seconds()
				print "%d requests remaining, reset in %d minutes..." % (remaining_requests,math.ceil(waiting_time/60.0))
				if remaining_requests == 0:
					print "Allowed requests depleted, waiting %d minutes and %d seconds before continuing..." % (math.floor(waiting_time/60.0),waiting_time % 60)
					time.sleep(waiting_time)
					continue
				if r.status_code != 200:
					print "Error, waiting 10 seconds before retrying..."
					time.sleep(10)
					continue
				users = json.loads(r.content)
				if not len(users):
					print "No more users returned, got em all :)"
					break
				params['since'] = str(users[-1]['id'])
				print "Added users: "+", ".join([str(user['id']) for user in users])
				for user in users:
					output_file.write(json.dumps(user).strip()+"\n")
	except KeyboardInterrupt:
		print "Quitting..."
		exit(0)
	finally:
		print "When relaunching this script, use the following minimum ID: ",params['since']
