from mq import capture
import CMQC
import random
import pymqi
import time

QUEUE_MANAGER = "QM.1"
CHANNEL = "SVRCONN.1"
HOST = "localhost"
PORT = "1434"

QUEUE_NAME = "TEST_QUEUE"
QUEUE_NAME2 = "TEST_QUEUE2"
QUEUE_NAME3 = "TEST_QUEUE3"
QUEUES = [QUEUE_NAME, QUEUE_NAME2, QUEUE_NAME3]
QUEUE_TYPE = CMQC.MQQT_LOCAL
QUEUE_MAX_DEPTH = "10000"

TEST_MESSAGE = "TEST MESSAGE"
TEST_MESSAGE2 = "TEST MESSAGE 2"
TEST_MESSAGE3 = "TEST MESSAGE 3"


class IntegrationTest:

	def set_up(self):
		self.connection_handler = capture.ConnectionHandler(QUEUE_MANAGER, CHANNEL, HOST, PORT)
		self.connection_handler.connect()
		for queue in QUEUES:
			self.connection_handler.create_queue(queue, QUEUE_TYPE, QUEUE_MAX_DEPTH)
			self.connection_handler.drain_queue(queue)		

	def put_message(self, queue_name, msg):
		self.connection_handler.put_message_in_queue(queue_name, msg)

	def get_message(self, queue_name):
		self.connection_handler.get_message_in_queue(queue_name)

	def tear_down(self):
		print "Exiting..."
		for queue in QUEUES:
			i = 0
			while i < 5:
				i+=1
				try:
					self.connection_handler.delete_queue(queue)
					break
				except:
					True #do nothing
		
		self.connection_handler.disconnect()


if __name__ == '__main__':
	print "CTRL+C to interrupt integration script.  This will run indefinitely until interrupted."
	integration_test = IntegrationTest()
	integration_test.set_up()
	
	try:
		i = 0
		while True:
			i+=1
			put_queue = QUEUES[random.randint(0,len(QUEUES)-1)]
			message = "A" * random.randint(1,10000)

			try:
				integration_test.put_message(put_queue, message)
			except pymqi.MQMIError:
				True #do nothing

			get_queue = QUEUES[random.randint(0,len(QUEUES)-1)]
			try:
				integration_test.get_message(get_queue)
			except pymqi.MQMIError:
				True
			
			if(i % 1000 == 0):
				print "%d messages queued and dequeued" % i

			time.sleep(1)

    	except KeyboardInterrupt:
		integration_test.tear_down()
	
	



