1.演示通过快照转发历史数据

[gcloud]
--在PubSub创建一个topic接收Kafka的消息，这个命令跑完之后PubSub有了Topic
gcloud pubsub topics create pubsub2dws_getfromkafka

--在PubSub创建一个订阅接收从PubSub创建的Topic发送来的消息，这个命令跑完之后PubSub有了subscription
gcloud pubsub subscriptions create pubsub2dws_subscription --topic=pubsub2dws_getfromkafka

[shell]
--在本地kafka创建一个消费组 连接上topic
cd C:\kafka\kafka_2.12-3.7.1
bin\windows\kafka-console-consumer.bat --bootstrap-server 192.1.0.127:9092 --group PubSubConsumerGroup --topic pubsub2dws_kafka --from-beginning 

连接使用数据
expected message test 1
expected message test 2
expected message test 3
expected message test 4

--查看消费组在本地
消费情况

2.演示消费数据同步，手动发送消息
