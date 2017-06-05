# -*- coding: UTF-8 -*-
'''
Author: bdang1@student.unimelb.edu.au
Filter tweets from a tweet_db view then save to a new couchdb on local instance
Criteria: 
- Reject retweets 
- Filter unwanted chars from text
- Get sentiment score for english tweets
'''
# +----------------------------------------------------------------------+

import getpass, settings, couchdb, json, pprint, re 
from couchdb.design import ViewDefinition
from textblob import TextBlob

pp = pprint.PrettyPrinter(indent=2)
# +----------------------------------------------------------------------+

'''
Access SENDER DB 
'''
ip_send   	 = 'localhost:5984'
dbname_send  = 'tweet_db'
user_send 	 = ''
pw_send   	 = getpass.getpass('Enter password for database %s: ' % dbname_send)
server_send  = 'http://' + user_send + ':' + pw_send + '@' + ip_send + '/'

view_send 	 = 'for_processing/new-view'
 
'''
Access to RECEIVER DB 
'''
ip_recv   	= 'localhost:5984'
dbname_recv	= 'processed_tweet_db'
user_recv 	= ''
pw_recv   	= getpass.getpass('Enter password for database %s: ' % dbname_recv)
server_recv = 'http://' + user_recv + ':' + pw_recv + '@' + ip_recv + '/'

# view_recv 	= 'eng-tweet/new-view'

'''
try connection
'''
try:
	db_send = couchdb.Server(server_send)[dbname_send]
	print "Successfully connected with ", db_send
except Exception as err:
	print "Unsuccessfully connect with ", server_send

try:
	db_recv = couchdb.Server(server_recv)[dbname_recv]
	print "Successfully connected with ", server_recv
except Exception as err:
	print "Unsuccessfully connect with ", server_recv

# +----------------------------------------------------------------------+

'''
Process the tweet text to remove unwanted symbols and characters 
'''
def remove_non_letters(text):
	# regex patterns
	hashtag	 = r'#\S+' 
	email	 = r'[\w\d\._-]+@\w+(\.\w+){1,3}'
	website	 = r'http\S+|www\.\w+(\.\w+){1,3}'
	retweet  = r'RT\s@\S+' 
	mention  = r'@[\w\d]+'
	punctual = r'[_\+-\.,!@\?#$%^&*();\\/|<>"\':]+'
	weird	 = r'�+'
	newline  = r'\n'
	spaces 	 = r'\s{2,}'
	digits   = r'\d+'

	combined_patterns = r'|'.join((hashtag, email, website, retweet, mention, punctual, weird, newline, digits))
	stripped = re.sub(combined_patterns, ' ', text)
	# remove extra whitespaces 
	stripped = re.sub(spaces, ' ', stripped)
	stripped = stripped.strip()
	return stripped

def remove_emojis(text):
	emoji_pattern = re.compile( 
	u"(\ud83d[\ude00-\ude4f])|"  # emoticons
	u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
	u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
	u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
	u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
	"+", flags=re.UNICODE)
	# return ''.join(c for c in str if c not in emoji.UNICODE_EMOJI)
	return emoji_pattern.sub(r' ', text)

def filter_tweet(text):
	text = remove_emojis(text)
	text = remove_non_letters(text)
	if not text or len(text) == 0: 
		return ''
	else: 
		return text 

'''
Do sentiment analysis 
'''
def get_sentiment(text):
	t = TextBlob(text)
	s = t.sentiment
	return s.polarity, s.subjectivity 


# +----------------------------------------------------------------------+
'''
# Load data from Couchdb
'''
def process_data_from_sender(db, view_name):
	docs  = []
	total = 0
	for i in db.view(view_name, reduce=False):
		tweet  = i.value # entire doc, type dict
		if 'retweeted_status' not in tweet:
			print "DOCTYPE: ", type(tweet)
			print '==='
			print tweet['text']
			print tweet['lang'], tweet['user']['lang']

			# -- processing -- 
			# filter text 
			fil_text  = filter_tweet(tweet['text'])
			print '- Filtered:%s' % fil_text

			# fields needed from a status
			wanted_info = {
				'coordinates' : tweet['coordinates'], 
				'created_at'  : tweet['created_at'],
				'favorite_count' : tweet['favorite_count'], 
				'lang' : tweet['lang'],
				'place': tweet['place'],
				'retweet_count': tweet['retweet_count'],
				'source': tweet['source'],
				'text': tweet['text'],
				'user': tweet['user'],
				'id_str': tweet['id_str']
			}

			# get sentiment score for eng tweet 
			if 'en' in tweet['lang']: # if tweet in English
				pol, subj = get_sentiment(fil_text) 
				community = tweet['user']['lang']
				print "- Assigned community: ", community
				print '==='

				# -- saving new doc --
				doc = {
					'_id' 			 : tweet['id_str'],
					'community_lang' : community,
					'polarity'		 : pol,
					'subjectivity'	 : subj
				}
				
			else: # if tweet not in English
				doc = {
					'_id' : tweet['id_str']
				}

			doc.update(wanted_info)

			docs.append(doc)
			print "- Len_doc: ", len(docs)
			# pp.pprint(doc)
			print '======================'

			if len(docs) % 20000 == 0:
				db_recv.update(docs)
				total += len(docs)
				print "Updated: " + str(len(docs)) + ', total: ' + str(total)
				docs = []

	if len(docs) > 0:
		db_recv.update(docs)
		total += len(docs)
		print "Updated: " + str(len(docs)) + ', total: ' + str(total)
	else:
		print "No tweets saved into " + dbname_recv

if __name__ == '__main__':
	process_data_from_sender(db_send, view_send)



	
	

















