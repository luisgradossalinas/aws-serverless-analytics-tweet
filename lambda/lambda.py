import json
import boto3
import base64
import os

sns = boto3.client("sns")
comprehend = boto3.client(service_name = 'comprehend')
dynamo = boto3.resource('dynamodb')
arn_topic = os.environ['arn_topic']

def lambda_handler(event, context):
    
    item = None
    table = dynamo.Table('tweet')
    
    decoded_record_data = [base64.b64decode(record['kinesis']['data']) for record in event['Records']]
    deserialized_data = [json.loads(decoded_record) for decoded_record in decoded_record_data]
    
    with table.batch_writer() as batch_writer:
    
        for item in deserialized_data:
            id_tweet = item['id_tweet']
            id_user = item['id_user']
            date_publish = item['date_publish']
            screen_name = item['screen_name']
            tweet = item['tweet']
            location = item['location']
            hashtag = item['hashtag']
            
            language = comprehend.detect_dominant_language(Text = tweet)
            code_language = language['Languages'][0]['LanguageCode']
            
            print("code_language : " + code_language)
            #Idiomas permitidos : 
            language_valid = ['hi', 'de', 'zh-TW', 'ko', 'pt', 'en', 'it', 'fr', 'zh', 'es', 'ar', 'ja']
            
            if code_language not in language_valid:
                continue
            
            sentiment_all = comprehend.detect_sentiment(Text = tweet, LanguageCode = code_language)
            sentiment = sentiment_all['Sentiment']
            
            batch_writer.put_item(                        
                Item = {
                            'id_tweet': id_tweet,
                            'id_user': id_user,
                            'date_publish': date_publish,
                            'screen_name' : screen_name,
                            'tweet' : tweet,
                            'location' : location,
                            'hashtag' : hashtag,
                            'sentiment' : sentiment
                        }
            )
            
            if sentiment == 'NEGATIVE':
                sns.publish(
                    TopicArn = arn_topic,    
                    Subject = '¡Tweet negativo detectado!',
                    Message = 'Se ha identificado que el usuario : ' + screen_name + " (https://twitter.com/" + screen_name + ") ha comentado lo siguiente (" + date_publish + "):" + "\n" + tweet
                )