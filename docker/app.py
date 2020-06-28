import sys
import boto3
import json
import tweepy
import uuid
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from datetime import datetime, timezone
import pytz

session = boto3.session.Session()
kinesis = boto3.client('kinesis', region_name = 'us-east-1')
secret = session.client(service_name = 'secretsmanager', region_name = 'us-east-1')

secret_name = "api_token_twitter"

hashtag = str(sys.argv[1])

response = secret.get_secret_value(SecretId = secret_name)

data = json.loads(response['SecretString'])
api_key = data['api_key']
api_secret_key = data['api_secret_key']
access_token = data['access_token']
access_token_secret = data['access_token_secret']

auth = tweepy.OAuthHandler(api_key, api_secret_key)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

stream_name = 'twitter-stream'

class KinesisStreamProducer(tweepy.StreamListener):

	def __init__(self, kinesis):
		self.kinesis = kinesis

	def on_data(self, data):
		data = json.loads(data)
		id_user = data['user']['id']
		screen_name = data['user']['screen_name']
		tweet = data['text']
		location = data['user']['location']
		data['created_at'] = datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=timezone.utc).astimezone(pytz.timezone('America/Lima')).strftime('%Y-%m-%d %H:%M:%S')
		date_publish = data['created_at']
		print("[INFO] El usuario " + screen_name + " (Tiene " + str(data['user']['followers_count']) + " seguidores y sigue a " + str(data['user']['friends_count']) + ") ha comentado sobre el hashtag : #" + hashtag)

		record = {
			'id_tweet': str(uuid.uuid4()),
			'id_user' : id_user,
			'date_publish' : date_publish,
			'screen_name' : screen_name,
			'tweet' : tweet,
			'location' : location,
			'hashtag' : hashtag
		}

		print(record)

		self.kinesis.put_record(
			StreamName = stream_name, 
			Data = json.dumps(record),
			PartitionKey = 'key'
		)

		print("Publishing in Kinesis Data Streams: ", tweet)
		return True

	def on_error(self, status):
		print("Error: " + str(status))

def main():
	mylistener = KinesisStreamProducer(kinesis)
	myStream = tweepy.Stream(auth = auth, listener = mylistener)
	myStream.filter(track=["#" + hashtag])

if __name__ == "__main__":
	main()