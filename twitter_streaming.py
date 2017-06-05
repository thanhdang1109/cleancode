'''
Author: Steve Dang 
Email: bdang1@student.unimelb.edu.au

Set up tweets harvesting with Twitter Stream API, tweepy
'''
# +----------------------------------------------------------------------+

import settings
import tweepy, json, couchdb, getpass, time

'''
Create a couchdb
'''
ip      = settings.SERVER_IP
db_name = settings.DB_NAME
user    = settings.DB_USERNAME
user_pw = getpass.getpass('Enter password for database %s: ' % db_name)

server  = 'http://' + user + ':' + user_pw + '@' + ip + '/'

tweetdb = couchdb.Server(server)[db_name] 

# +----------------------------------------------------------------------+

class StreamListener(tweepy.StreamListener):
    '''
    Create a class inherting from StreamListener 
    '''

    def on_data(self, data):

        # receive JSON msg from Twitter API
        data = json.loads(data)

        try: 
            doc = {
                '_id':data['id_str'],
                'text':data['text'],
                'coordinates':data['coordinates'],
                'user_name':data['user']['screen_name']
            }
            if data['place'] is not None:
                doc['place'] = data['place']['name']
            else:
                doc['place'] = None 

            print doc, '\n' 

            tweetdb.save(data)
            print doc, '\n'

        except Exception as err:
            print err

        return True

    def on_error(self, status):
        if status == 420:
            print "ERROR 420 - Twitter is limiting this account."
        else:
            print "ERROR - ", status 

    def on_timeout(self):
        print "Time out..."
             

def connect_API():

    # create a Stream object
    streamListener = StreamListener() 

    # Authenticating with Twitter Stream API
    print "Authenticating user..."

    auth = tweepy.OAuthHandler(settings.CONSUMER_KEY, settings.CONSUMER_SECRET)
    auth.set_access_token(settings.ACCESS_TOKEN, settings.ACCESS_TOKEN_SECRET)

    while True: 
        try: 
            stream = tweepy.Stream(auth, streamListener)
            print "Connected to Twitter API"
            print "Start streaming tweets..."
            stream.filter(locations=settings.TRACK_LOCATIONS)
        except:
            print "Connection failed. Sleeping for 1 minute and restart..."
            time.sleep(60)
            print "Restarting..."
            continue



# +----------------------------------------------------------------------+

if __name__ == '__main__':
    try: 
        connect_API()

    except KeyboardInterrupt:
        print "\n Admin interrupted with CTRL C."


   
    
