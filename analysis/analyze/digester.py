import json
import numpy
import os
class Digester:

	def process(self, snapshot_dir):
		snapshot_list = self.read_snapshot_files(snapshot_dir)
		return self.process_aggregate_stats(self.generate_aggregate_stats(snapshot_list))

	def read_snapshot_files(self, snapshot_dir):
		all_snapshots = []
		snapshots = os.listdir(snapshot_dir)

		if(len(snapshots) < 2):
			raise ValueError("Not enough snapshots to extrapolate results found in %s" % snapshot_dir)

		for snapshot in snapshots:
			with open(os.path.join(snapshot_dir, snapshot), 'r') as f:
				file_body = f.read()
				all_snapshots.append(json.loads(file_body))
		return all_snapshots
  		
	def generate_aggregate_stats(self, snapshots):
		snapshots = sorted(snapshots, key=lambda snapshot: snapshot["time"])
		if(len(snapshots) < 2):
			raise ValueError("Not enough stats to extrapolate results")

		processed_statistics = {}
		for i in range(0, len(snapshots)):
			captures = snapshots[i]["captures"]
			totalMsgIn, totalMsgOut, totalMsgDepth = 0,0,0
			msg_sizes = []
			persistent = []
			for j in range(len(captures)):
				
				totalMsgIn += captures[j]["msgIn"]
				totalMsgOut += captures[j]["msgOut"]
				totalMsgDepth += captures[j]["depth"]
				msg_sizes.append([ msg["size"] for msg in captures[j]["msgs"] ])
				persistent.append([ msg["persistent"] for msg in captures[j]["msgs"] ])

			msg_sizes = [item for sublist in msg_sizes for item in sublist]
			snapshots[i]["aggregateStats"] = { "totalMsgIn":totalMsgIn, "totalMsgOut":totalMsgOut, "totalMsgDepth":totalMsgDepth, "msgSizes":msg_sizes, "avgMessageSize":numpy.average(msg_sizes), "persistent":persistent}

		print snapshots
		return snapshots

	def process_aggregate_stats(self, snapshots):
		processed_stats = {"intervalStats":[], "overallStats":{"connStats":[]} }
		snapshots = sorted(snapshots, key=lambda snapshot: snapshot["time"])
		for j in range(1, len(snapshots)):
			interval_stat = {}
			start_time = snapshots[j-1]["time"]
			end_time = snapshots[j]["time"]
			interval = end_time - start_time

			interval_stat["msgInRate"] = float(snapshots[j]["aggregateStats"]["totalMsgIn"]) / float(interval)
			interval_stat["msgOutRate"] = float(snapshots[j]["aggregateStats"]["totalMsgOut"]) / float(interval)
			interval_stat["startTime"] = start_time
			interval_stat["endTime"] = end_time

			processed_stats["intervalStats"].append(interval_stat)

		all_msg_sizes = []
		all_persistent = []
		for j in range(0, len(snapshots)):
			all_msg_sizes.append(snapshots[j]["aggregateStats"]["msgSizes"])
			all_persistent.append(snapshots[j]["aggregateStats"]["persistent"])
			processed_stats["overallStats"]["connStats"].append({"time":snapshots[j]["time"], "connectionCount":snapshots[j]["connectionCount"]})

		all_depths = [ snapshot["aggregateStats"]["totalMsgDepth"] for snapshot in snapshots ]
		processed_stats["overallStats"]["allDepths"] = all_depths
		processed_stats["overallStats"]["avgDepth"] = numpy.average(all_depths)
		processed_stats["overallStats"]["minDepth"] = numpy.min(all_depths)
		processed_stats["overallStats"]["maxDepth"] = numpy.max(all_depths)
		processed_stats["overallStats"]["stdDevDepth"] = numpy.std(all_depths)

		all_msg_in_rates = [ stat["msgInRate"] for stat in processed_stats["intervalStats"] ]
		processed_stats["overallStats"]["avgMsgInRate"] = numpy.average(all_msg_in_rates)
		processed_stats["overallStats"]["minMsgInRate"] = numpy.min(all_msg_in_rates)
		processed_stats["overallStats"]["maxMsgInRate"] = numpy.max(all_msg_in_rates)
		processed_stats["overallStats"]["stdDevMsgInRate"] = numpy.std(all_msg_in_rates)

		all_msg_out_rates = [ stat["msgOutRate"] for stat in processed_stats["intervalStats"] ]
		processed_stats["overallStats"]["avgMsgOutRate"] = numpy.average(all_msg_out_rates)
		processed_stats["overallStats"]["minMsgOutRate"] = numpy.min(all_msg_in_rates)
		processed_stats["overallStats"]["maxMsgOutRate"] = numpy.max(all_msg_in_rates)
		processed_stats["overallStats"]["stdDevMsgOutRate"] = numpy.std(all_msg_in_rates)

		all_msg_sizes = [item for sublist in all_msg_sizes for item in sublist]
		processed_stats["overallStats"]["allMsgSizes"] = all_msg_sizes
		processed_stats["overallStats"]["avgMsgSize"] = numpy.average(all_msg_sizes)
		processed_stats["overallStats"]["maxMsgSize"] = numpy.max(all_msg_sizes)
		processed_stats["overallStats"]["minMsgSize"] = numpy.min(all_msg_sizes)
		processed_stats["overallStats"]["stdDevMsgSize"] = numpy.std(all_msg_sizes)
		
		processed_stats["overallStats"]["allPersistent"] = [item for sublist in all_persistent for item in sublist]

		all_conn_counts = [ conn_stat["connectionCount"] for conn_stat in processed_stats["overallStats"]["connStats"] ]
		processed_stats["overallStats"]["avgConnCount"] = numpy.average(all_conn_counts)

		return processed_stats
