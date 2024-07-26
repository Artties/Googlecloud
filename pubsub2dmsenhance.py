from google.cloud import pubsub_v1
from kafka import KafkaProducer
import time

# 订阅参数
project_id = "pubsub-connect-kafka"
subscription_names = ["pubsub2dws_subscription1", "pubsub2dws_subscription2"]
# Kafka Broker 信息
kafka_bootstrap_servers = '192.1.0.127:9092'
# Pub/Sub to Kafka topic mapping
topic_mapping = {
    "pubsub2dws_subscription1": "kafka_topic1",
    "pubsub2dws_subscription2": "kafka_topic2",
}

def receive_messages(subscription_name):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    def callback(message):
        # 处理接收到的消息
        message_data = message.data.decode('utf-8')
        print(f"Received message on {subscription_name}: {message_data}")
        # 发送消息到Kafka
        send_to_kafka(topic_mapping[subscription_name], message_data)
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

def send_to_kafka(kafka_topic, message):
    # 编写代码将消息发送到Kafka
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
    producer.send(kafka_topic, message.encode('utf-8'))
    producer.flush()
    producer.close()

if __name__ == '__main__':
    for subscription_name in subscription_names:
        receive_messages(subscription_name)
    # 持续监听消息
    while True:
        time.sleep(5)  # 使用 time 模块中的 sleep 函数

创建一个主题用于接收从PubSub传过来的所有消息
.\bin\windows\kafka-topics.bat --create --replication-factor 1 --partitions 1 --topic pubsub2dws_kafka --bootstrap-server 192.1.0.127:9092

