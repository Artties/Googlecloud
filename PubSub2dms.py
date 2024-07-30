from google.cloud import pubsub_v1
from kafka import KafkaProducer
import json
import threading
from datetime import timedelta
/*
*在这个脚本中，subscription_names 是一个包含多个订阅名称的列表。migrate_history_to_kafka 和 consume_from_pubsub 函数现在接受 subscription_name 作为参数，并针对每个订阅名称启动一个线程进行历史数据迁移和实时数据消费。这样可以处理多个订阅。  
*/
# Pub/Sub 订阅参数
project_id = "pubsub-connect-kafka"
subscription_names = ["pubsub2dws_subscription1", "pubsub2dws_subscription2"]  # 修改为多个订阅名称
kafka_bootstrap_servers = '192.1.0.127:9092'
kafka_topic = 'pubsub2dws_kafka'

# 初始化 KafkaProducer
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# 定义一个函数来列出快照并返回排序后的快照列表
def list_snapshots(subscriber, project_id):
    snapshots = list(subscriber.list_snapshots(request={"project": f"projects/{project_id}"}))
    snapshot_list = []
    # 计算 create_time = expire_time - 7天
    for snapshot in snapshots:
        if snapshot.expire_time:
            create_time = snapshot.expire_time - timedelta(days=7)
            snapshot_list.append({
                'snapshot': snapshot,
                'create_time': create_time
            })
    snapshot_list.sort(key=lambda x: x['create_time'].timestamp(), reverse=True)
    return snapshot_list

def migrate_history_to_kafka(subscription_name):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    # 获取 Pub/Sub 订阅的快照列表
    snapshots = list_snapshots(subscriber, project_id)

    # 选择最新的快照作为历史数据源
    if snapshots:
        snapshot_name = snapshots[0]['snapshot'].name
        print(f"Using snapshot: {snapshot_name} for subscription: {subscription_name}")

        # 手动拉取快照中的消息
        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": 10,  # 指定拉取的最大消息数量
            }
        )

        ack_ids = []
        for received_message in response.received_messages:
            try:
                callback(received_message.message)
                ack_ids.append(received_message.ack_id)
            except Exception as e:
                print(f"Error processing message: {e}")

        # 确认所有消息
        if ack_ids:
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
        print(f"Pulled messages from snapshot {snapshot_name} on {subscription_path}...")
    else:
        print(f"No snapshots found for subscription: {subscription_name}.")

def callback(message):
    # 处理接收到的消息
    message_data = message.data.decode('utf-8')
    print(f"Received message: {message_data}")

    # 发送消息到 Kafka
    send_to_kafka(message_data)

def send_to_kafka(message):
    try:
        # 发送消息到 Kafka
        producer.send(kafka_topic, message)
        producer.flush()
        print(f"Sent message to Kafka: {message}")
    except Exception as e:
        print(f"Failed to send message to Kafka: {e}")

def consume_from_pubsub(subscription_name):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...\n")

    # 调用 `result()` 以保持主线程处于活动状态
    try:
        streaming_pull_future.result()
    except:  # 捕获所有异常以防止线程中断
        streaming_pull_future.cancel()
        streaming_pull_future.result()

if __name__ == '__main__':
    # 创建多个线程用于历史数据迁移
    history_threads = []
    for subscription_name in subscription_names:
        history_thread = threading.Thread(target=migrate_history_to_kafka, args=(subscription_name,))
        history_threads.append(history_thread)
        history_thread.start()

    # 主线程用于消费实时数据
    for subscription_name in subscription_names:
        threading.Thread(target=consume_from_pubsub, args=(subscription_name,)).start()

    # 等待历史数据迁移线程完成
    for history_thread in history_threads:
        history_thread.join()
