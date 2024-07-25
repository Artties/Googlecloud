#这个脚本的作用是把所有pubsub发布的topics里的消息转发到本地kafka的topics里

from google.cloud import pubsub_v1
from kafka import KafkaProducer
import time  # 导入 time 模块

# 订阅参数
project_id = "pubsub-connect-kafka"
subscription_name = "pubsub2dws_subscription"
# Kafka Broker 信息
kafka_bootstrap_servers = '192.1.0.127:9092'

def receive_messages():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    def callback(message):
        # 处理接收到的消息
        message_data = message.data.decode('utf-8')
        print(f"Received message: {message_data}")
        # 发送消息到Kafka
        send_to_kafka(message_data)
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")
    # 持续监听消息
    while True:
        time.sleep(5)  # 使用 time 模块中的 sleep 函数

def send_to_kafka(message):
    # 编写代码将消息发送到Kafka
    # 这里可以使用Kafka的Python客户端库，如kafka-python

    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
    producer.send('pubsub2dws_kafka', message.encode('utf-8'))
    producer.flush()
    producer.close()

if __name__ == '__main__':
    receive_messages()
