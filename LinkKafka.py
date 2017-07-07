from kafka import KafkaProducer
import logging
import json


producer = KafkaProducer(bootstrap_servers=['csist-605c-1-R730:9092', 'csist-605c-2-R730:9092', 'csist-605c-3-R730:9092'])

log = logging.getLogger('PttGossipingCrawler.Kafka')


def send_json_kafka(file):
    try:
        if not isinstance(file, bytes):
            producer.send('News', file.encode())
        else:
            producer.send('News', file)
    except Exception as e:
        log.exception(e)
        log.error('Error when send json to kafka')

if __name__ == '__main__':
    s = '/data1/Ptt/Gossiping/20170704/20170704094925_Re: [問卦] 抱怨低薪的有認真唸書過嗎_windscore.json'
    with open(s, 'r') as f:
        js = json.load(f)
    send_json_kafka(json.dumps(js).encode())
