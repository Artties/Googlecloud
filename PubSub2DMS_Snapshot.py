from google.cloud import pubsub_v1
from kafka import KafkaProducer
import json
import threading
from datetime import timedelta
from queue import Queue

# Pub/Sub 订阅参数
project_id = 'pubsub-connect-kafka'
subscription_name = 'hc_sub'

# Kafka Broker 参数
kafka_bootstrap_servers = '111.119.208.59:9094'
kafka_topic = 'topic-546653556'

# 初始化 KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,  # 设置重试次数，以便处理 Kafka 中断或超时情况
    max_in_flight_requests_per_connection=5  # 控制 KafkaProducer 的并发请求数
)

# 使用队列来缓冲从 Pub/Sub 接收的消息，提高并发处理能力
message_queue = Queue()

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
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    # 获取 Pub/Sub 订阅的快照列表
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
            message_queue.task_done()  # 标记消息处理完成

if __name__ == '__main__':
    # 创建一个线程用于历史数据迁移
    history_thread = threading.Thread(target=migrate_history_to_kafka)
    history_thread.start()

    # 创建多个线程用于处理实时数据
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    realtime_threads = []
    for _ in range(5):  # 启动多个实时数据处理线程
        rt_thread = threading.Thread(target=process_realtime_data, args=(subscriber, subscription_path))
        rt_thread.start()
        realtime_threads.append(rt_thread)

    # 创建多个线程用于发送消息到 Kafka
    kafka_threads = []
    for _ in range(5):  # 根据实际需求设置发送消息的线程数
        kafka_thread = threading.Thread(target=send_to_kafka)
        kafka_thread.start()
        kafka_threads.append(kafka_thread)

    # 等待历史数据迁移线程完成
    history_thread.join()
    print("Historical data migration completed.")

    # 等待实时数据处理线程完成
    for rt_thread in realtime_threads:
        rt_thread.join()

    # 等待发送消息到 Kafka 的线程完成
    for kafka_thread in kafka_threads:
        kafka_thread.join()

    # 实时数据处理线程和发送消息到 Kafka 的线程会一直运行，除非显式终止
