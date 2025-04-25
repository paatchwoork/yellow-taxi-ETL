from kafka import KafkaProducer

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers = 'localhost:9092',
        value_serializer = lambda v: v.encode('utf-8')
    )
    topic_name = 'my-topic'
    message_value = 'Hello, Kafka!'

    def on_send_success(record_metadata):
        print(f"Message sent to topic: {record_metadata.topic}")
        print(f"Partition: {record_metadata.partition}")
        print(f"Offset: {record_metadata.offset}")

    def on_send_error(excp):
        print("Error during message send:", excp)

    producer.send(topic_name, message_value).add_callback(on_send_success).add_errback(on_send_error)


    # producer.send(topic_name,message_value)

    producer.flush()
    producer.close()