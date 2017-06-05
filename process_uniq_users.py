# -*- coding: UTF-8 -*-
'''
Author: bdang1@student.unimelb.edu.au
'''
# +----------------------------------------------------------------------+

import getpass, couchdb, json, pprint, re, requests, sys, time, numpy
from google.cloud import translate 
from collections import defaultdict

pp = pprint.PrettyPrinter(indent=2)
# +----------------------------------------------------------------------+

''' Access the view from source database '''
ip_send   	 = 'localhost:5984'
dbname_send  = 'processed_tweet_db'
user_send 	 = ''
pw_send 	 = ''
server_send  = 'http://' + user_send + ':' + pw_send + '@' + ip_send + '/' + dbname_send + '/'
view_tweets  = '_design/' + sys.argv[1] + '/_view/new-view'

''' Access to receiver database '''
ip_recv   	= 'localhost:5984'
dbname_recv	= 'processed_tweet_db'
user_recv 	= ''
pw_recv		= ''
server_recv = 'http://' + user_recv + ':' + pw_recv + '@' + ip_recv + '/' 

''' Try connecting to receiver database '''
try:
	db_recv = couchdb.Server(server_recv)[dbname_recv]
	print "Successfully connected with receiver", db_recv
except Exception as err:
	print "Unsuccessfully connect with receiver", db_recv

# +----------------------------------------------------------------------+
''' Load data from Couchdb '''

def get_docs_count(db, view):
	''' get the number of tweets in the view '''

	request = db + view 
	r = requests.get(request)
	content = json.loads(r.content)
	total_docs = content['rows'][0]['value']
	return total_docs

def request_docs(server, view, skip, limit):
	''' get doc from a Couchdb view '''

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

def set_unique_users_info(db, view, db_recv):
	''' Get coordinates and polarity score of unique users'''

	# dictionary to save all unique users info 
	unique_users = uniq = defaultdict(lambda: defaultdict(list))

	'''
	get all unique users info 
	'''
	# check how many docs to process in total 
	total_docs_from_view = get_docs_count(db, view)
	n = total_docs_from_view

	print "Total docs from view: ", total_docs_from_view

	total = 0
	for k in xrange(0, n/1000+1):
		skip  = k * 1000
		limit = 1000 
		
		docs = request_docs(db, view, skip, limit)

		for row in docs:
			info 	  = row['key']
			user_id   = info['user_id']
			longitude = float(info['coordinates'][0])
			latitude  = float(info['coordinates'][1])
			polarity  = float(info['polarity'])

			unique_users[user_id]['longitude'].append(longitude)
			unique_users[user_id]['latitude'].append(latitude)
			unique_users[user_id]['polarity'].append(polarity) 

			total += 1
		
		sys.stdout.flush()
		print "Got: " + str(len(docs)) + ', total so far: ' + str(total) + 'out of' + str(total_docs_from_view)
		print '======================'

		if total == total_docs_from_view:
			print "Finishing getting " + str(total) + "docs from " + str(total_docs_from_view) + "docs"
			break
	
	'''
	calculate average coordinates and polarity for each unique user
	'''
	for user in unique_users:
		info = unique_users[user]
		avg_longitude = numpy.mean(info['longitude'])
		avg_latitude  = numpy.mean(info['latitude'])
		avg_polarity  = numpy.mean(info['polarity'])

		unique_users[user]['avg_coordinates'] = [avg_longitude, avg_latitude]
		unique_users[user]['avg_polarity']    = avg_polarity

		print "avg_coordinates", unique_users[user]['avg_coordinates']
		print "avg_polarity", unique_users[user]['avg_polarity']
		print "---"

	print "=="*15
	print "START TO SAVE BACK TO DB"
	'''
	loop through the view again, and save the new info to couchdb
	'''
	queried = 0 
	unsaved = []

	for k in xrange(0, n/1000+1):
		skip  = k * 1000
		limit = 1000 
		
		docs = request_docs(db, view, skip, limit)

		for row in docs:
			doc_id  = row['id']
			doc     = db_recv.get(doc_id)
			user_id = row['key']['user_id']

			doc['avg_coordinates'] = unique_users[user_id]['avg_coordinates']
			doc['avg_polarity']    = unique_users[user_id]['avg_polarity']

			try: 
				db_recv.save(doc)
				print "Successful saving."
			except:
				print "Can't save."
				unsaved.append(doc_id)
				print "Unsaved", len(unsaved)
				continue

			queried += 1

			if queried == total_docs_from_view:
				sys.stdout.flush()
				return "Finishing updating unique users" + str(queried) + "docs from " + str(total_docs_from_view) + "docs"	

		
		sys.stdout.flush()
		print "Got: " + str(len(docs)) + ', total so far: ' + str(queried) + 'out of' + str(total_docs_from_view)
		print '======================'

		if queried == total_docs_from_view:
			return "Finishing updating unique users" + str(queried) + "docs from " + str(total_docs_from_view) + "docs"	

					
# +----------------------------------------------------------------------+

if __name__ == '__main__':
	set_unique_users_info(server_send, view_tweets, db_recv)


	




















