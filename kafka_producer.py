import kafka
import requests
import reddit_api
import time

config = configparser.ConfigParser()
config.read('api.cfg')

bootstrap_servers = 'localhost'
kafka_topic_name = 'api_topic'

#producer = KafkaProducer(bootstrap_servers=bootstrap_servers, 
#						 value_serializer=lambda v : json.dumps.encode('utf-8'))

json_message = None
column_1 = None
column_2 = None
column_3 = None

def get_subreddit_detail(search, payload):
	token = 'bearer ' + reddit_api.token
	base_url = 'https://oauth.reddit.com'
	headers = {'Authorization' : token, 'User-Agent' : 'etl-source by ' + config['api-info']['username']}
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
#while reddit_api.check_token():
while count < 2:
	if count == 0:
		json_message, last_item = get_subreddit_detail(search='/r/buildapcsales/hot', payload={'g': 'US', 'limit': 10})
	if count >= 1:
		json_message, last_item = get_subreddit_detail(search='/r/buildapcsales/hot', payload={'after': last_item, 'g': 'US', 'limit': 10})

	for i in range(len(json_message)):
		producer.send(kafka_topic_name, json_message)
		print("Published Message " + i)
		print("Wait for 2 seconds ... ")
		time.sleep(2)

	count += 1
print("end")

"""
GOAL:

Take 100 items per request from subreddit
- Check if the token is 200 and continue, this can be done using a with statement
- Avoid stickied post with an if statement:
	values['data']['children'][i]['data']['stickied'] == True
- Get whatever relevant information for that post, but place into a json format
- Want to do this iteratively and send to the kafka producer with a time.sleep in between messages

After a requests is completed and sent to the producer request information after
the last item using:
	values['data']['children'][i]['data']['name']

Note: Stay below 30 requests per minute
"""

