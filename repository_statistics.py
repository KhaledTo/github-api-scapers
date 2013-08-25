# -*- coding: utf-8 -*-

import json
import re
import sys
import gzip
from collections import defaultdict
import pprint

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: repository_statistics.py [input filename] "
        exit(-1)
    filename = sys.argv[1]


    if filename.endswith(".gz"):
        open_func = gzip.open
    else:
        open_func = open

    language_distribution =  defaultdict(lambda :0)

    with open_func(filename,"rb") as input_file:
        for line in input_file:
            try:
                user_repositories = json.loads(line)
                for repo in user_repositories:
                    if "language" in repo and repo["language"]:
                        language_distribution[repo["language"]]+=1
            except:
                print "Failed to load:"
                print line
                continue

    pprint.pprint(sorted(language_distribution.items(),key = lambda x:-x[1]))

    normalized_language_distribution = {}

    n_repos_total = sum(language_distribution.values())

    print "%d repositories in total" % n_repos_total

    for language,n_repos in language_distribution.items():
        normalized_language_distribution[language] = float(n_repos)/float(n_repos_total)*100

    pprint.pprint(sorted(normalized_language_distribution.items(),key = lambda x:-x[1]))
