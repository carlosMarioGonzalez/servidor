# example_consumer.py
import pika, os, csv
from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate a Token from the "Tokens Tab" in the UI
token = "MlUoRzyipVkKqyGSjil7696heOcs8s4JDI_IVNWKqvZ5_eVAbaht16Fwwm46oJN0PRUQnu9-L7W0qhpgoAjNFA=="
org = "taller 3"
bucket = "dispositivo1"

client = InfluxDBClient(url="http://52.234.212.255:8086", token=token)


def process_function(msg):
  mesage = msg.decode("utf-8")
  print(mesage)
  write_api = client.write_api(write_options=SYNCHRONOUS)

  data = "mem,host=host1 used_percent=" + mesage
  write_api.write(bucket, org, data)
  return

while 1:
  url = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@rabbit:5672/%2f')
  params = pika.URLParameters(url)
  connection = pika.BlockingConnection(params)
  channel = connection.channel() # start a channel
  channel.queue_declare(queue='mensajes') # Declare a queue
  # create a function which is called on incoming messages
  def callback(ch, method, properties, body):
    process_function(body)

  # set up subscription on the queue
  channel.basic_consume('mensajes',
    callback,
    auto_ack=True)

  # start consuming (blocks)
  channel.start_consuming()
  connection.close()