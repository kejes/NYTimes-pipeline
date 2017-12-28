from pykafka import KafkaClient
import json
import datetime as dt
import urllib3

file_path = "/home/u220850/news_flash_pipeline/"

print(dt.datetime.now().strftime(format='%c'))

def decode_http_response(r):
    assert r.status == 200, 'HTTP request did not return a valid status code'
    return r.data.decode()

http = urllib3.PoolManager()
api_key = "8e1ff87091aa47dc9b35742e1445065d" # Add own API key
meta_data_url = "http://api.nytimes.com/svc/news/v3/content/all/all.json?api-key=" + api_key
meta_data_url_offset = url = "http://api.nytimes.com/svc/news/v3/content/all/all.json?" + urllib3.request.urlencode({
    'api-key': api_key,
    'offset': 20})  # Helper method that translates key-value pairs into a url query

r = http.request('GET', url=meta_data_url)  # To fetch meta-data of articles
r2 = http.request('GET', url=meta_data_url_offset)  # Fetch 20 new articles

all_recent_articles = json.loads(decode_http_response(r))  # Type = dictionary
all_recent_articles_offset = json.loads(decode_http_response(r2))  # Type = dict
combined = all_recent_articles['results'] + all_recent_articles_offset['results'] # Type array of JSON objects

print("Results fetched")

client = KafkaClient(hosts="127.0.0.1:9092") # Default port 9092
topic = client.topics[b'kafka_nyt'] # select or create new topic

with topic.get_sync_producer() as producer:
    producer.produce(str.encode(json.dumps(combined))) # Write array of JSON file

print("Files written...done")

