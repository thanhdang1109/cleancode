# +----------------------------------------------------------------------+

try:
    from twitter_creds import *
except Exception:
    pass 

import pandas as pd
def get_lang():
	f = r'language-codes.csv'
	df = pd.read_csv(f, names = ['Codes', 'Langs'])
	langs = df['Codes'].tolist()

	# List unwanted languagues
	unwanted_langs = ['en']

	wanted_langs = [c for c in langs if c not in unwanted_langs]
	
	return wanted_langs 

# +----------------------------------------------------------------------+


# DATABASE SETTINGS 
'''
Server IP and couchdb are manually created
server  = 'http://' + DB_USERNAME + ':' + user_pw + '@' + ip + '/'
'''
SERVER_IP	= '115.146.87.112:5984'
DB_NAME 	= 'tweet_db'
DB_USERNAME	= ''

# longitude, latitude of locations
# source: http://boundingbox.klokantech.com (choose CSV)
MELBOURNE   = [144.951435,-37.855527,144.989097,-37.799446]
SYDNEY      = [150.520929,-34.118347,151.343021,-33.578141] 
AUSTRALIA 	= [110.95,-54.83,159.29,-11.35]               

# API TRACKING SETTINGS 
TRACK_TERMS         = []
TRACK_LOCATIONS     = MELBOURNE 
TRACK_LANG			= get_lang()

 # +----------------------------------------------------------------------+
# print TRACKING_LANG
# print TRACK_LOCATIONS



