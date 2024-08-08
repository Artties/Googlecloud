from google.cloud import pubsub_v1
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import json
from datetime import timedelta
from queue import Queue
import concurrent.futures

# Pub/Sub 订阅参数
project_id = 'pubsub-connect-kafka'
subscription_name = 'hc_sub'

# Kafka Broker 参数
kafka_bootstrap_servers = '111.119.208.59:9094'
kafka_topic = 'topic-546653556'
consumer_group_id = 'my-consumer-group'  # 消费组 ID

# 初始化 KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,  # 设置重试次数，以便处理 Kafka 中断或超时情况
    max_in_flight_requests_per_connection=5  # 控制 KafkaProducer 的并发请求数
)

# 使用队列来缓冲从 Pub/Sub 接收的消息，提高并发处理能力
message_queue = Queue()

# 初始化 Pub/Sub SubscriberClient
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# 初始化 KafkaConsumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_bootstrap_servers,
    group_id=consumer_group_id,
    auto_offset_reset='latest',  # 使用最新模式，从最新的消息开始消费
    enable_auto_commit=True,  # 自动提交偏移量
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# 订阅 Kafka 主题
consumer.subscribe(topics=[kafka_topic])

# 定义一个函数来列出快照并返回排序后的快照列表
def list_snapshots(subscriber, project_id):
    snapshots = list(subscriber.list_snapshots(request={"project": f"projects/{project_id}"}))
    snapshot_list = []
    for snapshot in snapshots:
        if snapshot.expire_time:
            create_time = snapshot.expire_time - timedelta(days=7)
            snapshot_list.append({
                'snapshot': snapshot,
                'create_time': create_time
            })
    snapshot_list.sort(key=lambda x: x['create_time'].timestamp(), reverse=True)
    return snapshot_list

# 处理历史数据的函数
def migrate_history_to_kafka():
    snapshots = list_snapshots(subscriber, project_id)

    # 选择最新的快照作为历史数据源
    if snapshots:
        snapshot_name = snapshots[0]['snapshot'].name
        print(f"Using snapshot: {snapshot_name}")

        # 手动拉取快照中的消息
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": 1000,  # 增大批量处理的消息数量
            }
        )

        ack_ids = []
        for received_message in response.received_messages:
            try:
                message_data = received_message.message.data.decode('utf-8')
                print(f"Received message from snapshot: {message_data}")
                message_queue.put(message_data)  # 将消息放入队列
                ack_ids.append(received_message.ack_id)
            except Exception as e:
                print(f"Error processing message: {e}")

        # 确认所有消息
        if ack_ids:
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
        print(f"Pulled messages from snapshot {snapshot_name} on {subscription_path}...")
    else:
        print("No snapshots found.")

# 处理实时数据的函数
def process_realtime_data(subscriber, subscription_path):
    while True:
        # 模拟实时数据处理，这里可以根据实际需求修改
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": 10,  # 控制每次拉取的消息数量
            }
        )

        ack_ids = []
        for received_message in response.received_messages:
            try:
                message_data = received_message.message.data.decode('utf-8')
                print(f"Received message in real-time: {message_data}")
                message_queue.put(message_data)  # 将消息放入队列
                ack_ids.append(received_message.ack_id)
            except Exception as e:
                print(f"Error processing real-time message: {e}")

        # 确认所有消息
        if ack_ids:
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
        print(f"Processed real-time messages on {subscription_path}...")

# 发送消息到 Kafka 的函数
def send_to_kafka():
    while True:
        message_data = message_queue.get()  # 从队列获取消息
        try:
            # 发送消息到 Kafka
            producer.send(kafka_topic, message_data)
            producer.flush()
            print(f"Sent message to Kafka: {message_data}")
        except Exception as e:
            print(f"Failed to send message to Kafka: {e}")
        finally:
            message_queue.task_done()

# 消费来自 Kafka 的数据的函数
def consume_from_kafka():
    for message in consumer:
        message_data = message.value
        try:
            # 在这里处理来自 Kafka 的消息，可以根据需要进行处理
            print(f"Consumed message from Kafka: {message_data}")
        except Exception as e:
            print(f"Error processing message from Kafka: {e}")
        finally:
            # 自动提交偏移量
            consumer.commit()

# 主函数，用于启动历史数据处理、实时数据处理、发送消息到 Kafka 和消费从 Kafka 接收的数据
def main():
    # 使用线程池来管理并发任务
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # 提交历史数据处理任务到线程池
        future_history = executor.submit(migrate_history_to_kafka)

        # 提交实时数据处理任务到线程池
        future_realtime = executor.submit(process_realtime_data, subscriber, subscription_path)

        # 提交发送消息到 Kafka 的任务到线程池
        future_sender = executor.submit(send_to_kafka)

        # 启动消费者线程处理从 Kafka 接收的数据
        executor.submit(consume_from_kafka)

        # 等待所有任务完成
        concurrent.futures.wait([future_history, future_realtime, future_sender])

if __name__ == "__main__":
    main()
