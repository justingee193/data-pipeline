import requests
import configparser

config = configparser.ConfigParser()
config.read('api.cfg')

def get_token(username, password, id, secret, headers):
	base_url = 'https://www.reddit.com/'
	data = {'grant_type' : 'password', 'username' : username, 'password' : password}
	auth = requests.auth.HTTPBasicAuth(id, secret)
	r = requests.post(base_url + 'api/v1/access_token', data=data, headers=headers, auth=auth)
	return r.json()['access_token']

def check_token():
	access_token = (get_token(username=config['api-info']['username'], password=config['api-info']['password'], 
					id=config['api-info']['app_id'], secret=config['api-info']['app_secret'], 
					headers={'user-agent': 'etl-source by ' + config['api-info']['username']}))
	token = 'bearer ' + access_token
	base_url = 'https://oauth.reddit.com'
	headers = {'Authorization' : token, 'User-Agent' : 'etl-source by ' + config['api-info']['username']}
	response = requests.get(base_url + '/api/v1/me', headers=headers)
	status = response.status_code

	if status == 200:
		return True
	else:
		return False

token = get_token(username=config['api-info']['username'], password=config['api-info']['password'], 
				  id=config['api-info']['app_id'], secret=config['api-info']['app_secret'], 
				  headers={'user-agent': 'etl-source by ' + config['api-info']['username']})