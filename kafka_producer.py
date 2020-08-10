import kafka
import requests
import reddit_api
import time

config = configparser.ConfigParser()
config.read('api.cfg')

bootstrap_servers = 'localhost:9093'
kafka_topic_name = 'reddit-posts'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v : json.dumps.encode('utf-8'))

def get_subreddit_detail(search, payload):
	token = 'bearer ' + reddit_api.token
	base_url = 'https://oauth.reddit.com'
	headers = {'Authorization' : token, 'User-Agent' : config['api-info']['app_name'] + ' by ' + config['api-info']['username']}
	response = requests.get(base_url + search, headers=headers, params=payload)
	values = response.json()

	json_message = []
	last_item = str()
	for i in range(len(values["data"]["children"])):
		if values['data']['children'][i]['data']['stickied']:
			continue
		post = values["data"]["children"][i]["data"]
		title = post["title"]
		num_comments = post["num_comments"]
		upvotes = post["ups"]
		upvote_ratio = post["upvote_ratio"]
		url = post["url"]
		post_created = post["created_utc"]
		message = {

			"title": title, 
			"num_comments": num_comments, 
			"upvotes": upvotes, 
			"upvote_ratio": upvote_ratio,
			"url": url, 
			"post_created": post_created

		}
		json_message.append(message)

		last_item = post["name"]
	return json_message, last_item

count = 0
while reddit_api.check_token():
	if count == 0:
		json_message, last_item = get_subreddit_detail(search='/r/buildapcsales/hot', payload={'g': 'US', 'limit': 100})
	if count >= 1:
		json_message, last_item = get_subreddit_detail(search='/r/buildapcsales/hot', payload={'after': last_item, 'g': 'US', 'limit': 100})

	for i in range(len(json_message)):
		producer.send(kafka_topic_name, json_message)
		print("Published Message " + i)
		print("Wait for 2 seconds ... ")
		time.sleep(2)

	if count == 10:
		break

print("Publishing Stopped")