# encoding:utf-8
import time
import random
import string
import json

__author__ = 'aihua.sun'

import logging
from kafka import KafkaProducer
from kafka import KafkaClient

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    # 如果配置filename就会输出到文件，否则就在console
                    # filename='myapp.log',
                    filemode='w')
hosts = "localhost:9092"
topic = 'test_flink_table'


class KafkaSender():

    def __init__(self):
        # self.producer = KafkaProducer(bootstrap_servers=hosts, compression_type='lz4')
        self.producer = KafkaProducer(bootstrap_servers=hosts)
        # self.client.ensure_topic_exists(topic)

    def send_messages(self, msg):
        self.producer.send(topic, msg)
        self.producer.flush()


def get_instance():
    return KafkaSender()


if __name__ == "__main__":
    begin = time.time()
    producer = get_instance()
    for i in range(0, 100):
        user={"name":"eric%s"%i,"age":random.randint(1, 50),"address":"wuhan%s"%i}
        producer.send_messages(json.dumps(user))
    end = time.time()
    print("use time:" + str((end - begin)))
