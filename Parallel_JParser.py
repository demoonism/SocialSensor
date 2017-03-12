import gzip
import json
import time
#import simplejson as json
import sys
import os
import glob
import datetime

def parseJson(jsonfile):
    tweets = []
    #counter = 0

    with gzip.open(jsonfile,'r') as fin:
        for line in fin:
            #if "entities" not in line:
            #    continue
            #print(len(line))
            if len(line) > 1000: # check if the line is proper tweet (by observation, all valid tweets have length at least 1000.)
                d = json.loads(line.decode("UTF-8"))  # more effecient way would be check the line validity before loading
                #print(len(d))
                ###### 21 in windows......this line is really ad hoc...but it works...
                #if len(d) == 21 and len(d['entities']['hashtags']): ## explicit booleaness to check is a list is empty
                #print(d)
                if len(d['entities']['hashtags']): ## explicit booleaness to check is a list is empty
        
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
                                 "lang":d['user']['lang'],
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
                #if counter % 100000 == 0:
                #    print "processed ", counter
                #counter+=1
    return tweets




if __name__ == "__main__":

    #print(sys.argv)
    start_time = time.time()

    ## Linux
    #jsonfilename = '/home/danielshi/tw_pyspark/Sample/statuses.log.2014-07-01-00.gz'
    folder = sys.argv[1]    #'/mnt/f/Demoonism/Data Scientist/Spark/statuses.log.2014-07-01-00.gz'
    # /mnt/1e69d2b1-91a9-473c-a164-db90daf43a3d/Backup_tw_2013_2_8/2013-02/2013-02-01
    print("running folder ",folder, "..." )
    ## Windows
    #jsonfilename = '../statuses.log.2014-07-01-00.gz'
    path = os.path.join(folder,'*.gz')
    output = []
    files = glob.glob(path)

    counter = 0
    for jsonfilename in files[:1]:
        #print "processing ", counter
        result = parseJson(jsonfilename)
        #print result
        #output.append(result)
        output = output + result  ##simple list concatenation
        counter += 1
        if counter % 2 == 0:
            print "processed ", counter
        out_filename = sys.argv[2]
        #out_filename = 'out'
        sec = time.time() - start_time
        m, s = divmod(sec, 60)
        h, m = divmod(m, 60)
        print('Pre-processing '+ folder +' takes - %d:%02d:%02d which is %d seconds in total' % (h,m,s,sec))
        with gzip.open(out_filename+'.json.gz', 'wb') as f:
            for eachjson in output:
                json.dump(eachjson, f)
                f.write('\n')
                
                
#            for eachjson in output:
#                for eachline in eachjson:
#                    f.write(eachline)
#                    f.write('\n')
                #json.dump(out, f)
                #json.dump(output, f,indent=1)
                #f.write(out)
                #f.write('\n')