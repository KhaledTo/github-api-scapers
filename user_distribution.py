# -*- coding: utf-8 -*-

import json
import re
import sys
import gzip
from collections import defaultdict
import pprint

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: number_of_german_users.py [input filename] [country distribution output directory|optional]"
        exit(-1)
    filename = sys.argv[1]
    if len(sys.argv) >= 3:
        output_directory = sys.argv[2]
    else:
        output_directory = None
    total_users = 0
    hireable_users = 0

    if filename.endswith(".gz"):
        open_func = gzip.open
    else:
        open_func = open

    user_distribution =  defaultdict(lambda :0)
    usernames_and_emails_by_country = defaultdict(lambda : [])
    countries = []

    with open("iso_country_codes.txt","r") as iso_codes_file:
        iso_codes_file.readline()
        for line in iso_codes_file:
            if not line.strip():
                break
            (full_name,iso_code) = [x.lower().strip() for x in line.strip().split(";")]
            if full_name.find(",") != -1:
                (name,title) = full_name.split(",")
            else:
                name = full_name
            countries.append((name,iso_code))

    cities_by_country = defaultdict(lambda :[])

    with gzip.open("world_cities.txt.gz","r") as world_cities_file:
        world_cities_file.readline()
        for line in world_cities_file:
            if not line.strip():
                break
            (iso_code,city_name,accented_city_name,region,population,latitude,longitude) = [x.lower() for x in line.split(",")]
            if population and int(population) > 100000:
                sanitized_name = re.sub(r"[^\w\d\s]+","",city_name)
                print sanitized_name,str(population)
                cities_by_country[iso_code].append((sanitized_name,population))
    compiled_countries = [(re.compile("(\A|[^\w]+)%s(\Z|[^\w]+)" % i[0],re.I),i[1]) for i in countries]
    compiled_cities_by_country = defaultdict(lambda : [])
    for country,cities in cities_by_country.items():
        compiled_cities_by_country[country] = [(re.compile("(\A|[^\w]+)%s(\Z|[^\w]+)" % x[0].replace(" ",r"\s+"),re.I),x[0],x[1]) for x in cities]

    print cities_by_country
    with open_func(filename,"rb") as input_file:
        for line in input_file:
            try:
                user_details = json.loads(line)
            except:
                print "Failed to load:"
                print line
                continue
            total_users+=1
            found = False
            if 'location' in user_details and user_details['location']:
                for country_re,iso_code in compiled_countries:
                    if re.search(country_re,user_details['location']):
                        user_distribution[iso_code] += 1
                        print iso_code,user_details['location']
                        found = True
                    
                    if not found:
                        max_population = 0
                        for city,city_name,population in compiled_cities_by_country[iso_code]:
                            if re.search(city,user_details['location']) and population > max_population:
                                print iso_code,user_details['location'],city_name,population
                                user_distribution[iso_code] += 1
                                found = True
                                max_population = population

                    
                    if found == True:
                        break
            if not found:

                for country_re,iso_code in compiled_countries:
                    if 'email' in user_details and user_details['email'] and user_details['email'].lower().endswith("."+iso_code):
                        print iso_code,user_details['email']
                        user_distribution[iso_code] += 1
                        found = True
                        break

            if found and 'email' in user_details and user_details['email']:
                usernames_and_emails_by_country[iso_code].append((user_details['login'],user_details['name'] or '(name not given)',user_details['hireable'],user_details['email'],user_details['location'] or '(unknown location)',user_details['public_repos'],user_details['followers']))

            if not found:
                user_distribution["unknown_location"] += 1

    pprint.pprint(sorted(user_distribution.items(),key = lambda x:-x[1]))

    classified_users = user_distribution.copy()

    del classified_users['unknown_location']

    total_classified_users = sum(classified_users.values())

    if output_directory:
        for country,usernames_and_emails in usernames_and_emails_by_country.items():
            with open(output_directory+"/"+country+".txt","w") as output_file:
                for username,name,hireable,email,location,public_repos,followers in usernames_and_emails:
                    encoded_strs = [x.encode("utf8") for x in (username.replace(";","/"),name.replace(";","/"),u'hireable' if hireable else u'NOT hireable',email,location.replace(";","/"),str(public_repos),str(followers))]
                    output_file.write(";".join(encoded_strs))
                    output_file.write("\n")

    pprint.pprint(sorted([(x[0],float(x[1])/float(total_classified_users),x[1]) for x in classified_users.items()],key = lambda x:-x[1]))
