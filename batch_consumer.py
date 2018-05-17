import pika
import queue
import time
import json
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
channel = connection.channel()
channel.queue_declare(queue='final', durable=True)

global msg_queue
global t
maxsize=6
msg_queue=queue.Queue()

def empty_queue():

    global t
    print(time.time())
    l=[]
    i=int(msg_queue.qsize())
    while i!=0:
        l.append(msg_queue.get())
        i-=1
    if t.isAlive():
        t.cancel()
    t=threading.Timer(1,empty_queue)
    if len(l)>0:
        logger.info("Data pushed at ", str(time.time()))
        dummy_worker(l)
    t.start()



t=threading.Timer(1,empty_queue)
logger.info("Consumer started at ", str(time.time()))
t.start()


def dummy_worker(data_list):
    print('data list', data_list)
    time.sleep(0.2)

while True:
    try:
        if int(msg_queue.qsize())<maxsize:
            consume_generator = channel.consume(queue='final', no_ack=True)
            result=next(consume_generator)
            data=json.loads(result[2].decode("utf-8"))
            if isinstance(data,dict):
                msg_queue.put(data)
            else:
                for z in data:
                    msg_queue.put(z)


        else:
            t.cancel()
            empty_queue()
    
    except Exception as e:

        logger.info("Decoding JSON has failed due to ", str(e))
