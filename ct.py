from confluent_kafka import Consumer, KafkaError
from k_meta_message import meta_message

c = Consumer({'bootstrap.servers': '***-example.com', 'group.id':'randomgroupgen', 'default.topic.config':{'auto.offset.reset':'smallest'}})
c.subscribe(['__consumer_offsets'])

print("starting consumer");
running = True

mtmsg = meta_message()

while running:
	msg = c.poll()
	print("pulled")
	if not msg.error():
		print(mtmsg.parse(msg.value()))
	elif msg.error().code() != KafkaError._PARTITION_EOF:
		print(msg.error())
		running = False
c.close()
