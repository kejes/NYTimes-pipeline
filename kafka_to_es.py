import datetime as dt
from elasticsearch import Elasticsearch
from pykafka import KafkaClient
import json

print(dt.datetime.now().strftime(format='%c'))

client = KafkaClient(hosts="127.0.0.1:9092") # Default port 9092

client.topics # list all currently existing topics
topic = client.topics[b'kafka_nyt'] # select or create new topic

consumer = topic.get_simple_consumer(consumer_timeout_ms=5000) # Let it stop when nothing is returned
data_to_es = [json.loads(message.value) for message in consumer if message is not None] # Get a list with all json obj
print("results fetched from Kafka")

es = Elasticsearch()

for article in data_to_es[-1]: # Retrieve all json objects (i.e. articles) from last message (40 in total)
    es_dict={}
    es_dict['abstract'] = article['abstract']
    es_dict['section'] = article['section']
    es_dict['subsection'] = article['subsection']
    es_dict['published_date'] = article['published_date']
    es_dict['url'] = article['url']
    es_dict['title'] = article['title']
    weekday = article['first_published_date'].split('T')[0]
    es_dict['weekday'] = dt.datetime.strptime(weekday, '%Y-%m-%d').strftime('%A')
    try:
        es_dict['geo_facet'] = article['geo_facet'][0] # Check if this is no empty string
    except:
        es_dict['geo_facet'] = 'unknown'
    res = es.index(index="nyt3", doc_type='article', id=article.get('url', None), body=es_dict)

# Retrieve one from ES for testing
es.get(index="nyt3", id='https://www.nytimes.com/2017/11/29/reader-center/clearing-the-air-on-climate-education-a-reporter-returns-to-the-scene-of-her-story-ohio.html', doc_type='article')    
    
print("results written to ES, done")