from confluent_kafka import Consumer, KafkaError
from k_meta_message import meta_message
from k_meta_message import meta_message

class offset_consumer(self, shared_values, offset):
	c = Consumer({'bootstrap.servers':'localhost:9092', 'group.id': 'k_meta-group', 'default.topic.config': {'auto.offset.reset':'smallest'}})
	c.subscribe(['__consumer_offsets'])
	running = True

	def start():
		mtmsg = meta_message()
		while running:
			msg = c.poll()
			if not msg.error():
				offset_meta = mtmsg.parse(msg.value())
				if len(offset_meta) > 0:
					for ips in offset_meta[0]:
						try:
							shared_values.ip.index(ips)
						except ValueError:
							shared_values.ip.append(ips)
				if len(offset_meta) > 1:
					for ct in offset_meta[1]:
						try:
							shared_values.offset_consumer.index(ct)
						except ValueError:
							shared_values.offset_consumer.append(ct)
			elif msg.error() != KafkaError._PARTITION_EOF:
				print(msg.error())
				running = False
		c.close()

	def stop():
		running = False

