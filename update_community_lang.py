# -*- coding: UTF-8 -*-
'''
Author: bdang1@student.unimelb.edu.au
Update the community_lang field from langugage code to readable name
e.g: 'zh' -> 'Chinese'
'''
# +----------------------------------------------------------------------+
import getpass, couchdb, json, pprint, re, requests, sys, time
from couchdb.design import ViewDefinition
from textblob import TextBlob
import pandas as pd

pp = pprint.PrettyPrinter(indent=2)
# +----------------------------------------------------------------------+

'''
Access SENDER DB 
'''
ip_send   	 = 'localhost:5984'
dbname_send  = 'processed_tweet_db'
user_send 	 = ''
pw_send 	 = ''
server_send  = 'http://' + user_send + ':' + pw_send + '@' + ip_send + '/' + dbname_send + '/'
view_send 	 = '_design/community-langs/_view/new-view'

url = server_send + view_send

print "SENDER: ", url 

'''
Access to RECEIVER DB 
'''
ip_recv   	= 'localhost:5984'
dbname_recv	= 'processed_tweet_db'
user_recv 	= ''
pw_recv		= ''
server_recv = 'http://' + user_recv + ':' + pw_recv + '@' + ip_recv + '/'

'''
try connecting to receiver database
'''
try:
	db_recv = couchdb.Server(server_recv)[dbname_recv]
	print "Successfully connected with receiver", server_recv
except Exception as err:
	print "Unsuccessfully connect with receiver", server_recv

# +----------------------------------------------------------------------+
def get_twitter_langcode(twitter_file, allcodes_file):
	'''get the language code and readable name from a csv file put in a dictionary'''
	df_twt = pd.read_csv(twitter_file, names = ['Lang', 'Code'])
	df_all = pd.read_csv(allcodes_file, names = ['Code', 'Lang'])
	t = df_twt['Code'].tolist()
	a = df_all['Code'].tolist()
	dif = [i for i in t if i not in a]
	d = {}
	for i, r in df_all.iterrows():
		print r['Code'], r['Lang']
		d[r['Code']] = r['Lang']
		print "in d=: ", d[r['Code']]
		print "--"
	print "Add the addition from twitter: "
	for i in dif:
		print df_twt.loc[df_twt['Code'] == i]['Lang'].values
		d[i] = df_twt.loc[df_twt['Code'] == i]['Lang'].values
		print d[i], '\n'
	return d

# +----------------------------------------------------------------------+
'''
# Load data from Couchdb
'''

def get_docs_count(db, view):
	''' get the number of tweets in the view '''
	request = db + view 
	r = requests.get(request)
	content = json.loads(r.content)
	total_docs = content['rows'][0]['value']
	return total_docs

def get_tweets(server, view, skip, limit):
	request = '?skip={}&limit={}&reduce=false'.format(skip, limit)
	r = requests.get(server + view + request)
	if r.status_code == 200:

		try:
			response = json.loads(r.content)
			docs = response['rows']
			return docs 

		except Exception as e:
			print '-- invalid json object --'
			print 'sleeping for 10 seconds'
			time.sleep(10)
			sys.stdout.flush()
			return get_tweets(server, view, skip,limit)	

	else:
		print '-- server did not respond --'
		print 'sleeping for 10 seconds then restart'
		time.sleep(10)
		sys.stdout.flush()
		return get_tweets(server, view, skip,limit)

def process_data_from_sender(db, view_name):
	# get language names
	lang_names = get_twitter_langcode('twitter_langcode.csv', 'language-codes.csv')

	# check how many docs to process in total 
	total_docs_from_view = get_docs_count(db, view_name)
	n = total_docs_from_view

	total = 0
	not_saved = []
	for k in xrange(0, n/1000+1):
		skip  = k * 1000
		limit = 1000 
		docs = get_tweets(server_send, view_send, skip, limit)

		for row in docs:
			doc_id    = row['id']
			doc    	  = db_recv.get(doc_id)
			if 'community_lang' in doc:
				if 'community' in doc:
					print "Current community: ", doc['community']
				else:
					try: 
						code = doc['community_lang']
						community = lang_names[code]
						doc['community'] = community
						db_recv.save(doc)
						print code, '--->', community
						print "=====\n" 
					except Exception:
						print "Can't save document"
						not_saved.append(doc_id)
						pass
			total += 1

			print "Total " + str(total) + '/' + str(n) + 'Unsaved:' + str(len(not_saved)) + '\n'

			if total == total_docs_from_view:
				sys.stdout.flush()
				print "UNSAVED: "
				for i in not_saved:
					print i
				return "Finishing updating " + total + "docs from " + str(n) + "docs"


if __name__ == '__main__':
	process_data_from_sender(server_send, view_send)
	
	









