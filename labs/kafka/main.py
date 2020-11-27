from confluent_kafka import  KafkaException, KafkaError
import sys
import json
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer


class Transaction(object):
    def __init__(self, id=None, order_type=None, amount=None, price=None):
        self.id = id
        self.order_type = order_type
        self.amount = amount
        self.price = price

    def __repr__(self):
        return 'id: '+str(self.id) + '  price:' + str(self.price)

    def __str__(self):
        return 'id: '+str(self.id) + '  price' + str(self.price)


def dict_to_transaction(obj):
    if obj is None:
        return None

    id = int(obj['data']['id'])
    order_type = int(obj['data']['order_type'])
    amount = float(obj['data']['amount'])
    price = float(obj['data']['price'])
    return Transaction(id, order_type, amount, price)

def main():
    string_deserializer = StringDeserializer('utf_8')
    conf = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'bitcoin_group',
            'key.deserializer': string_deserializer,
            'value.deserializer': string_deserializer,
            'session.timeout.ms': 6000,
            'fetch.wait.max.ms': 5000,
            'auto.offset.reset': 'smallest',
            'enable.auto.commit': 'false',
            'fetch.min.bytes': 307200}

    consumer = DeserializingConsumer(conf)
    consumer.subscribe(['bitcoin-transaction'])
    messages =[]
    try:
        while True:
            msg = consumer.poll(timeout=1000)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                obj = json.loads(msg.value())
                transaction = dict_to_transaction(obj)
                messages.append(transaction)
                if len(messages) > 100:
                    messages = sorted(messages, key=lambda x: x.price, reverse=True)[0:10]
                    print(messages)
                consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == '__main__':
    main()
