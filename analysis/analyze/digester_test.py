import unittest
import digester
import shutil
import json
import tempfile
import os

CLEANUP = True
class TestDigester(unittest.TestCase):

	def setUp(self):
		self.digester = digester.Digester()

	def test_json_reader(self):
		tmp_dir = tempfile.mkdtemp()
		with open(os.path.join(tmp_dir, "1.json") , 'w') as outfile:
			json.dump({"test1":1}, outfile)
		with open(os.path.join(tmp_dir, "2.json") , 'w') as outfile:
			json.dump({"test2":2}, outfile)
		with open(os.path.join(tmp_dir, "3.json") , 'w') as outfile:
			json.dump({"test3":3}, outfile)

		all_snapshots = self.digester.read_snapshot_files(tmp_dir)
		self.assertEquals(3, len(all_snapshots))

		if CLEANUP: shutil.rmtree(tmp_dir)

	def test_json_reader_no_dir(self):
		self.assertRaises(OSError, self.digester.read_snapshot_files, "/not/a/directory/lol")

	def test_json_reader_no_files(self):
		tmp_dir = tempfile.mkdtemp()
		self.assertRaises(ValueError, self.digester.read_snapshot_files, tmp_dir)
		shutil.rmtree(tmp_dir)

	def test_generate_aggregate_1_snapshot(self):
		self.assertRaises(ValueError, self.digester.generate_aggregate_stats, [ { "time":1000, "connectionCount": 20, "captures":[{ "queueName": "Q.1", "msgIn":30, "msgOut":27, "depth":3, "msgs":[{"size":20, "persistent":True},{"size":18,"persistent":True}]}, { "queueName": "Q.2", "msgIn":30, "msgOut":27, "depth":3, "msgs":[{"size":20, "persistent":True},{"size":18,"persistent":True},{"size":40, "persistent":True},{"size":58,"persistent":True}]}]  } ])

	def test_generate_aggregates(self):
		stats = self.digester.generate_aggregate_stats([ { "time":1000, "connectionCount": 10, "captures":[{ "queueName": "Q.1", "msgIn":30, "msgOut":27, "depth":3, "msgs":[{"size":20, "persistent":True},{"size":18,"persistent":True}]}, { "queueName": "Q.2", "msgIn":30, "msgOut":27, "depth":3, "msgs":[{"size":20, "persistent":True},{"size":18,"persistent":True},{"size":40, "persistent":True},{"size":58,"persistent":True}]}]}, { "time":2000, "connectionCount":21, "captures":[{ "queueName": "Q.1", "msgIn":50, "msgOut":33, "depth":17, "msgs":[{"size":20, "persistent":True},{"size":18,"persistent":True}]}, { "queueName": "Q.2", "msgIn":40, "msgOut":27, "depth":13, "msgs":[{"size":20, "persistent":True},{"size":18,"persistent":True},{"size":40, "persistent":True},{"size":58,"persistent":True}]}]  } ])

	def test_process_aggregates(self):
		stats = self.digester.process_aggregate_stats([{'captures': [{'depth': 3, 'msgIn': 30, 'queueName': 'Q.1', 'msgs': [{'persistent': True, 'size': 20}, {'persistent': True, 'size': 18}], 'msgOut': 27}, {'depth': 3, 'msgIn': 30, 'queueName': 'Q.2', 'msgs': [{'persistent': True, 'size': 20}, {'persistent': True, 'size': 18}, {'persistent': True, 'size': 40}, {'persistent': True, 'size': 58}], 'msgOut': 27}], 'connectionCount': 10, 'aggregateStats': {'totalMsgOut': 54, 'totalMsgDepth': 6, 'persistent': [[True, True], [True, True, True, True]], 'totalMsgIn': 60, 'msgSizes': [20, 18, 20, 18, 40, 58], 'avgMessageSize': 29.0}, 'time': 1000}, {'captures': [{'depth': 17, 'msgIn': 50, 'queueName': 'Q.1', 'msgs': [{'persistent': True, 'size': 20}, {'persistent': True, 'size': 18}], 'msgOut': 33}, {'depth': 13, 'msgIn': 40, 'queueName': 'Q.2', 'msgs': [{'persistent': True, 'size': 20}, {'persistent': True, 'size': 18}, {'persistent': True, 'size': 40}, {'persistent': True, 'size': 58}], 'msgOut': 27}], 'connectionCount': 21, 'aggregateStats': {'totalMsgOut': 60, 'totalMsgDepth': 30, 'persistent': [[True, True], [True, True, True, True]], 'totalMsgIn': 90, 'msgSizes': [20, 18, 20, 18, 40, 58], 'avgMessageSize': 29.0}, 'time': 2000}])
	
if __name__ == '__main__':
  unittest.main()
