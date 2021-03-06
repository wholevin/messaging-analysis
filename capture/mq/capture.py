import argparse
import fnmatch
import json
import os
import pymqi
import sys
from time import time, sleep
import CMQC, CMQCFC, CMQXC

ALL_PREFIX = "*"
DEFAULT_DIR = "results"
QUEUE_STATS_KEY, CONN_COUNT_KEY, QUEUE_DEPTH_KEY, MSG_IN_KEY, MSG_OUT_KEY = "queueStats", "connCount", "queueDepth", "msgIn", "msgOut"

def parse_command_line_arguments():
	parser = argparse.ArgumentParser(description="Captures the message statistics for the provided queue manager.")

	#Positional required arguments
	parser.add_argument("queue_manager", help="The name of the queue manager")
	parser.add_argument("interval", type=int, help="The number of seconds between each capture")

	#Optional arguments
	parser.add_argument("-a", "--address", default="localhost", help="The host that the queue manager is located at")
	parser.add_argument("-c", "--channel", default="SVRCONN.1", help="The channel to use to connect to the queue manager")
	parser.add_argument("-o", "--output", help="The directory to write the captured statistics to. Defaults to a new temp directory.") 
	parser.add_argument("-p", "--port", default=1434, help="The port to connect to")
	parser.add_argument("-r", "--runs", type=int, help="The number of times to run the capture. This may not be used with the time option.")
	parser.add_argument("-t", "--time", type=int, help="Amount of time to run the capture in seconds. This may not be used with the runs option.")

	args = parser.parse_args()

	if args.runs and args.time:
		raise RuntimeError, "The arguments -r/--runs and -t/--time cannot both be specified"    
	if not args.runs and not args.time:
		raise RuntimeError, "The argument -r/--runs or -t/--time is required"

	return args

def default_dir():
	if(not os.path.exists(DEFAULT_DIR)):
		os.mkdir(DEFAULT_DIR)
	return DEFAULT_DIR

def capture_statistics(connection_handler):
	queues = connection_handler.get_all_queues()
	queue_statistics = []
	for queue in queues:
		queue_name = queue[CMQC.MQCA_Q_NAME]
		try:
			messages = connection_handler.browse_messages_in_queue(queue_name)
			statistics = connection_handler.get_queue_statistics(queue_name)
			queue_statistics.append(QueueStatistics(queue_name, messages, statistics))
		except pymqi.MQMIError, e:
			print "Failed to read queue '%s'" % queue_name.split() 

	return {QUEUE_STATS_KEY: queue_statistics, CONN_COUNT_KEY: connection_handler.get_connection_count()}
	
def output_to_file(output_dir, statistics, i):
	f = open(os.path.join(output_dir,"cap_%s.json" % i), "w")
	f.write(Captures(statistics).to_json())
	f.close()
	
#Class to handle the connection to the Websphere MQ broker
class ConnectionHandler:

	def __init__(self, queue_manager, channel, host, port):
		self.queue_manager = queue_manager
		self.cd = pymqi.CD()
		self.cd.ChannelName = channel
		self.cd.ConnectionName = "%s(%s)" % (host, port)
		self.cd.ChannelType = CMQC.MQCHT_CLNTCONN
		self.cd.TransportType = CMQC.MQXPT_TCP

	def browse_messages_in_queue(self, queue_name):
		queue = pymqi.Queue(self.connection, queue_name, CMQC.MQOO_BROWSE)

		message_descriptors = pymqi.md()
		get_message_options = pymqi.gmo()
		get_message_options.Options = CMQC.MQGMO_BROWSE_NEXT

		messages = []
		while True:
			try:
				message_body = queue.get(None, message_descriptors, get_message_options)
				messages.append(Message(message_descriptors, message_body))

				#These are required in order to move the cursor to the next entry.
				message_descriptors['MsgId'] = ''
				message_descriptors['CorrelId'] = ''
			except pymqi.MQMIError, e:
				if e.comp == CMQC.MQCC_FAILED and e.reason == CMQC.MQRC_NO_MSG_AVAILABLE:
					break
				else:
					raise

		queue.close()

		return messages
		
	def connect(self):
		self.connection = pymqi.PCFExecute(name = None)
		self.connection.connect_with_options(self.queue_manager, cd=self.cd, opts=CMQC.MQCNO_HANDLE_SHARE_BLOCK)
		
	def create_queue(self, queue_name, queue_type, max_depth):
		args = {CMQC.MQCA_Q_NAME: queue_name, CMQC.MQIA_Q_TYPE: queue_type, CMQC.MQIA_MAX_Q_DEPTH: max_depth}
		try:
			self.connection.MQCMD_CREATE_Q(args)
		except pymqi.MQMIError, e:
			if e.reason == CMQCFC.MQRCCF_OBJECT_ALREADY_EXISTS:
				print "Warning: Queue '%s' already exists on queue manager '%s'!" % (queue_name, self.queue_manager)
			else:
				raise

	def delete_queue(self, queue_name):
		args = {CMQC.MQCA_Q_NAME: queue_name}
		self.drain_queue(queue_name)
		self.connection.MQCMD_DELETE_Q(args)

	def disconnect(self):
		self.connection.disconnect()

	def drain_queue(self, queue_name):
		queue = pymqi.Queue(self.connection, queue_name)

		while True:
			try:
				queue.get()
			except pymqi.MQMIError, e:
				if e.comp == CMQC.MQCC_FAILED and e.reason == CMQC.MQRC_NO_MSG_AVAILABLE:
					break
				else:
					raise
		
		queue.close()
	
	def get_message_in_queue(self, queue_name):
		queue = pymqi.Queue(self.connection, queue_name)
		message = queue.get()
		queue.close()
		return message

	def get_queue_statistics(self, queue_name):
		stats = {}
		try:
			queue = pymqi.Queue(self.connection, queue_name)
			stats[QUEUE_DEPTH_KEY] = queue.inquire(CMQC.MQIA_CURRENT_Q_DEPTH)
			
			#To get msg enq and deq each time, the stats must be reset using the following call
			response = self.connection.MQCMD_RESET_Q_STATS({CMQC.MQCA_Q_NAME: queue_name})

			stats[MSG_IN_KEY] = response[0][CMQC.MQIA_MSG_ENQ_COUNT]
			stats[MSG_OUT_KEY] = response[0][CMQC.MQIA_MSG_DEQ_COUNT]


		except pymqi.MQMIError, e:
			if e.comp == CMQC.MQCC_FAILED and e.reason== CMQC.MQRC_UNKNOWN_OBJECT_NAME:
				print "The queue '%s' was not found." % queue_name.strip()
			else:
				raise

		return stats

	def get_all_queues(self):
		args = {CMQC.MQCA_Q_NAME: ALL_PREFIX, CMQC.MQIA_Q_TYPE: CMQC.MQQT_LOCAL}
		return self.connection.MQCMD_INQUIRE_Q(args)

	def put_message_in_queue(self, queue_name, message):
		queue = pymqi.Queue(self.connection, queue_name)
		queue.put(message)
		queue.close()
	
	def get_connection_count(self):
		return self.connection.MQCMD_INQUIRE_Q_MGR_STATUS()[0][CMQCFC.MQIACF_CONNECTION_COUNT]


class JSONSerializable(object):

	def to_json(self):
        	return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

class Message(JSONSerializable):
		
	def __init__(self, header, body):
		self.parse_header(header, body)

	def parse_header(self, header, body):
		self.size = len(body)
		if header['Persistence'] == 1:
			self.persistent = True
		else:
			self.persistent = False


#Json parseable class holding the overall statistics
class Captures(JSONSerializable):

	def __init__(self, statistics):
		self.time = time()
		self.connectionCount = statistics[CONN_COUNT_KEY]
		self.captures = statistics[QUEUE_STATS_KEY]

#Json parseable class holding the queue statistics
class QueueStatistics(JSONSerializable):

	def __init__(self, queue_name, messages, statistics):
		self.queueName = queue_name.split()[0]
		self.depth = statistics[QUEUE_DEPTH_KEY]
		self.msgIn = statistics[MSG_IN_KEY]
		self.msgOut = statistics[MSG_OUT_KEY]
		
		self.msgs = messages

if __name__ == '__main__':
	args = parse_command_line_arguments()

	connection_handler = ConnectionHandler(args.queue_manager, args.channel, args.address, args.port)
	connection_handler.connect()
	
	output_dir = args.output	
	if not output_dir:
		output_dir = default_dir()
	output_dir =  os.path.abspath(output_dir)

	output_files = os.listdir(output_dir)
	if len(output_files) > 0:
		choice = ""
		while(choice not in ['y','n']):
			
			choice = raw_input("%s contains output files already.  Clear directory (NOTE: This will clear all files in the directory; BE AWARE)? (y/n)" % output_dir).lower()
			if choice == 'y':
				non_result_files = []				
				for output_file in output_files:
					if not fnmatch.fnmatch(output_file, '*cap_*.json'):
						non_result_files.append(output_file)

				if(len(non_result_files)):
					print "Non result file(s) exist in %s: [%s]. Aborting.  Please choose a different output directory." % (output_dir,str.join(", ", non_result_files))
					sys.exit()
				
				for root, dirs, files in os.walk(output_dir):
				    for f in files:
				    	os.unlink(os.path.join(root, f))

			elif choice == 'n':
				print "Aborting.  Please choose a different output directory."
				sys.exit()
			else:
				print "Invalid choice: %s" % choice
		
				
	if args.runs:
		i = 1
		times = 0
		while times < args.runs:
			queue_statistics = capture_statistics(connection_handler)
			output_to_file(output_dir, queue_statistics, i)
			times += 1
			i+=1
			sleep(args.interval)
	else:
		end_time = time() + args.time
		while time() < end_time:
			queue_statistics = capture_statistics(connection_handler)
			i+=1
			output_to_file(output_dir, queue_statistics, i)
			time.sleep(args.interval)

	for dirpath, dirnames, filenames in os.walk(output_dir):
	    for filename in filenames:
		os.chmod(os.path.join(dirpath, filename), 0o777)

	print "Your results are located in directory: %s" % output_dir

	

