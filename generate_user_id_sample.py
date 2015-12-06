import json
import sys
import subprocess
import random
import re
import time

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print "Usage: generate_user_id_sample.py [user ID filename] [output filename] [sample size]"
        exit(-1)
    input_filename = sys.argv[1]
    output_filename = sys.argv[2]
    n_sample = int(sys.argv[3])

    random.seed(time.time())

    line_count_str = subprocess.check_output(["wc","-l",input_filename])
    match = re.match(r"^(\d+)",line_count_str)
    line_count = int(match.group(1))
    print "{0:d} lines in input file, selecting a random sample of {1:d}".format(line_count, n_sample)
    indices = sorted(random.sample(xrange(0,line_count),n_sample))
    with open(input_filename,"rb") as input_file, \
         open(output_filename,"wb") as output_file:
        for i,line in enumerate(input_file):
            if i < indices[0]:
                continue
            print i
            del indices[0]
            output_file.write(line)
            if not indices:
                break
