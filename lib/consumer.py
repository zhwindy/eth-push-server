#!/usr/bin/env python
# encoding=utf-8
import pika
import json

credentials = pika.PlainCredentials('admin', '123456')
connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1', 5672, 'app', credentials))
channel = connection.channel()
channel.exchange_declare(exchange='app.user.push.msg', exchange_type='topic', durable=True)

channel.queue_declare(queue='app.user.push.btc.queue', durable=True)


def callback(ch, method, properties, body):
    with open('transactions.txt', 'a') as f:
        f.write(json.dumps(json.loads(body.decode()), indent=4))


channel.basic_consume(callback, queue='app.user.push.btc.queue', no_ack=True)

print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
