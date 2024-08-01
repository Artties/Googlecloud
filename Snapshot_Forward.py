from google.cloud import pubsub_v1
from kafka import KafkaProducer
import json
import threading
from datetime import timedelta

# Pub/Sub 订阅参数
project_id = 'pubsub-connect-kafka'
subscription_name = 'pubsub2dws_subscription'

# Kafka Broker 参数
kafka_bootstrap_servers = '111.119.208.59:9094'
kafka_topic = 'pubsub2dws'

# 初始化 KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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
                "max_messages": 10,  # 指定拉取的最大消息数量
            }
        )

        ack_ids = []
        for received_message in response.received_messages:
            try:
                message_data = received_message.message.data.decode('utf-8')
                print(f"Received message from snapshot: {message_data}")
                send_to_kafka(message_data)
                ack_ids.append(received_message.ack_id)
            except Exception as e:
                print(f"Error processing message: {e}")

        # 确认所有消息
        if ack_ids:
            subscriber.acknowledge(request={"subscription": subscription_path, "ack_ids": ack_ids})
        print(f"Pulled messages from snapshot {snapshot_name} on {subscription_path}...")
    else:
        print("No snapshots found.")

def send_to_kafka(message):
    try:
        # 发送消息到 Kafka
        producer.send(kafka_topic, message)
        producer.flush()
        print(f"Sent message to Kafka: {message}")
    except Exception as e:
        print(f"Failed to send message to Kafka: {e}")

if __name__ == '__main__':
    # 创建一个线程用于历史数据迁移
    history_thread = threading.Thread(target=migrate_history_to_kafka)
    history_thread.start()
    
    # 等待历史数据迁移线程完成
    history_thread.join()
