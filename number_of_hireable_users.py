import json
import re
import sys
import gzip

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: number_of_german_users.py [input filename]"
        exit(-1)
    filename = sys.argv[1]
    total_users = 0
    hireable_users = 0

    if filename.endswith(".gz"):
        open_func = gzip.open
    else:
        open_func = open

    with open_func(filename,"rb") as input_file:
        for line in input_file:
            try:
                user_details = json.loads(line)
            except:
                print "Failed to load:"
                print line
                continue
            total_users+=1
            if 'hireable' in user_details and user_details['hireable'] == True and user_details['email']:
                print "Hireable: %s (%s)" % (user_details['login'],user_details['email'] if 'email' in user_details and user_details['email'] else '[no email given]')
                hireable_users+=1
            
    print "Found %d hireable users from %d users in total (%2.2f %%)." % (hireable_users,total_users,float(hireable_users)/float(total_users)*100)
