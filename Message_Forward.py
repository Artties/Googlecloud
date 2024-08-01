from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from confluent_kafka import Producer

# 配置 Google Cloud Pub/Sub
project_id = 'pubsub-connect-kafka'
subscription_id = 'pubsub2dws_subscription'

# 配置华为云 DMS-Kafka
# kafka_conf = {
#     'bootstrap.servers': '192.1.0.94:9011',
#     'sasl.mechanism': 'PLAIN',
#     'security.protocol': 'SASL_SSL',
#     'sasl.username': 'admin',
#     'sasl.password': 'Mic45034622@',
#     'client.id': 'pubsub-migration-client'  # 这里可以设置为任何你想要的字符串
# }


kafka_config = {
    'bootstrap.servers': '111.119.208.59:9094',
    'client.id': 'PubSubToKafka'
}
kafka_topic = 'pubsub2dws'


# 创建 Pub/Sub 订阅者
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# 创建 Kafka 生产者
producer = Producer(kafka_config)

# 回调函数处理从 Pub/Sub 接收到的消息并转发到 Kafka
def callback(message):
    try:
        print(f"Received message from Pub/Sub: {message.data.decode('utf-8')}")
        producer.produce(kafka_topic, value=message.data)
        producer.flush()
        message.ack()
        print(f"Message forwarded to Kafka topic {kafka_topic}")
    except Exception as e:
        print(f"Failed to forward message to Kafka: {e}")
        message.nack()

# 启动 Pub/Sub 订阅
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

# 使用一个长时间运行的脚本来保持订阅活动状态
try:
    streaming_pull_future.result()
except TimeoutError:
    streaming_pull_future.cancel()
    streaming_pull_future.result()

# 清理资源
subscriber.close()
producer.close()
