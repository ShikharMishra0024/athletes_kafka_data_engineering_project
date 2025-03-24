from confluent_kafka import Consumer, Producer

BROKER_URL = "broker_id:9092"
def create_consumer(grp_id : str, client_id : str, topic : list[str]) -> Consumer:
    consumerConf = {
    "bootstrap.servers":BROKER_URL,\
    "group.id":grp_id,\
    "client.id":client_id,\
    "enable.auto.commit":True,\
    "auto.commit.interval.ms":5000,\
    "auto.offset.reset":"earliest"   
    }
    consumer = Consumer(consumerConf)
    consumer.subscribe(topic)
    return consumer
def create_producer(client_id:str) -> Producer:
    conf = {"bootstrap.servers":"192.168.18.113:9092",\
        "client.id":"producer-1",\
        "linger.ms":1000,\
        "batch.num.messages":100,\
        "compression.type":"snappy",\
        "acks":"all",\
        "message.timeout.ms":300000,\
        "request.timeout.ms":30000,\
        "retry.backoff.ms":500,\
        "retry.backoff.max.ms":2000,\
        "enable.idempotence":True}
    return Producer(conf)