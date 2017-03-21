import gzip
import json
import time
#import simplejson as json
import sys
import os
import glob
import datetime
from datetime import datetime



def cleanText(x):
    return not (x.startswith("#")|x.startswith("@")|x.startswith("http:")| (x == '\n')| (x == '\r'))

def getUnixTimeStamp(stamp):
    d = datetime.strptime(stamp,'%a %b %d %H:%M:%S +0000 %Y')
    unixtime = time.mktime(d.timetuple())
    return unixtime


def parseJson(jsonfile):
    tweets = []

    with gzip.open(jsonfile,'r') as fin:
        for line in fin:

            if len(line) > 1000: # check if the line is proper tweet (by observation, all valid tweets have length at least 1000.)
                d = json.loads(line.decode("UTF-8"))  # more effecient way would be check the line validity before loading

                if len(d['entities']['hashtags']): ## explicit booleaness to check is a list is empty
        
                    ## dow = day of week.  unpacking timestamp into individual vars 
                    ##(or maybe I should do this in the next phase?  note this is still in unicode)
                    #dow, month, day, time,_,year = d['created_at'].split(" ") 
                    ## Assign the extracted values into a new json to be stored.
                                 ## UserName --> from_user
                    term_list = filter(cleanText, d['text'].split(" "))

                    processed = {"from_user":d['user']['screen_name'],
                                 ## Split hashtag, we only want the text in hashtag, discard indices.
                                 "hashtag":[hash_string['text'] for hash_string in d['entities']['hashtags']], 
                                 ## Split terms in tweet text, remove \n and \r
                                 "term": [w.replace('\n', '').replace('\r','') for w in d['text'].split(" ")], 
                                 ## append loc_ to each word in location
                                 #"location":['loc_' + s for s in d['user']['location'].split(" ")],
                                 "location":['loc_' + s for s in d['user']['location'].split(" ")],
                                 ## mention ids
                                 "mention":[mention['screen_name'] for mention in d['entities']['user_mentions']],
                                 ## language
                                 "lang":d['user']['lang'],
                                 "HashTag_Birthday":getUnixTimeStamp(d['created_at'])
                                }
                    tweets.append(processed)

    return tweets


def toGizpJson(filename):
    with gzip.open(filename+'.json.gz', 'wb') as f:
        for eachjson in output:
            json.dump(eachjson, f) # only works for json, cannot write(json)
            f.write('\n')
            
def getTime(start):
    sec = time.time() - start
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    return s,m,h, sec

if __name__ == "__main__":

    #print(sys.argv)
    start_time = time.time()

    ## Linux
    #jsonfilename = '/home/danielshi/tw_pyspark/Sample/statuses.log.2014-07-01-00.gz'
    input_arg = sys.argv[1]    #'/mnt/f/Demoonism/Data Scientist/Spark/statuses.log.2014-07-01-00.gz'
    output_arg = '/mnt/66e695cd-1a0c-4e3b-9a50-55e01b788529/Tweet_Output' 
    # /mnt/1e69d2b1-91a9-473c-a164-db90daf43a3d/Backup_tw_2013_2_8/2013-02/2013-02-01
    print("running folder ",input_arg, "..." )
    ## Windows
    #jsonfilename = '../statuses.log.2014-07-01-00.gz'
    
    counter = 0
    
    for item in os.listdir(input_arg):
        for dayfile in os.listdir(os.path.join(input_arg, item)):
            now = time.time() ## timer set!
            
            folder = os.path.join(input_arg, item, dayfile)
            path = os.path.join(folder,'*.gz')
            files = glob.glob(path)
            out_filename = os.path.join(output_arg, item, dayfile)
            
            output = []
            print('Total of ', len(files),' files')
            for jsonfilename in files:
                
                result = parseJson(jsonfilename)
                output = output + result  ##simple list concatenation
                counter += 1
                if counter % 5 == 0:
                    print "processed ", counter
            
            
            s,m,h,sec = getTime(now)
            print('Pre-processing '+ dayfile +' takes - %d:%02d:%02d which is %d seconds in total' % (h,m,s,sec))
        
            toGizpJson(out_filename)
                
    s,m,h,sec = getTime(start_time)
    print('Finally....Pre-processing the entire folder takes - %d:%02d:%02d which is %d seconds in total' % (h,m,s,sec))                