import gzip
#import json
import time
import simplejson as json
import sys

def parseJson(jsonfile):
    tweets = []
    with gzip.open(jsonfile,'r') as fin:
        counter = 0
        for line in fin:
            #if "entities" not in line:
            #    continue
            d = json.loads(line)  # more effecient way would be check the line validity before loading
            if len(d) == 24 and len(d['entities']['hashtags']): ## explicit booleaness to check is a list is empty
                
                ## dow = day of week.  unpacking timestamp into individual vars 
                ##(or maybe I should do this in the next phase?  note this is still in unicode)
                dow, month, day, time,_,year = d['created_at'].split(" ") 
                ## Assign the extracted values into a new json to be stored.
                             ## UserName --> from_user
                processed = {"from_user":d['user']['screen_name'],
                             ## Split hashtag, we only want the text in hashtag, discard indices.
                             "hashtag":[hash_string['text'] for hash_string in d['entities']['hashtags']], 
                             ## Split terms in tweet text, remove \n and \r
                             "term": [w.replace('\n', '').replace('\r','') for w in d['text'].split(" ")], 
                             ## append loc_ to each word in location
                             "location":['loc_' + s for s in d['user']['location'].split(" ")],
                             ## mention ids
                             "mention":d['entities']['user_mentions'],
                             ## language
                             "lang":d['lang'],
                             ## day of week
                             "dow":dow,
                             ## month in 3 letters
                             "month": month,
                             ## numeric day 1-31
                             "day":day,
                             ## hr:min:sec
                             "time":time,
                             ## numeric year
                             "year":year
                            }
                tweets.append(processed)
                #tweets.append(d)
            if counter % 50000 == 0:
                print "processed ", counter
            counter+=1
    return tweets




if __name__ == "__main__":

	## Linux
	#jsonfilename = '/home/danielshi/tw_pyspark/Sample/statuses.log.2014-07-01-00.gz'
	jsonfilename = sys.argv[1]    #'/mnt/f/Demoonism/Data Scientist/Spark/statuses.log.2014-07-01-00.gz'
	## Windows
	#jsonfilename = '../statuses.log.2014-07-01-00.gz'

	start_time = time.time()
	result = parseJson(jsonfilename)
	print('Pre-processing A takes: {} seconds'.format(round(time.time() - start_time, 5)))

	#with open('data.txt', 'w') as outfile:
	#    json.dump(result, outfile)
		
	with gzip.open('../out.json.gz', 'wb') as f:
		json.dump(result, f)



