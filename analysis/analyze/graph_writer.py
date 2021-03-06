from reportlab.pdfgen import canvas
from reportlab.lib.units import inch, cm
import os
import tempfile
import matplotlib.pyplot as plt
import fnmatch

class GraphWriter:

	def process(self, stats, output_dir):
		self.create_graphs(stats, output_dir)
		return self.write_pdf(output_dir, stats)
		

	def write_pdf(self, file_loc, stats):
		pdf_loc = os.path.join(file_loc,'report.pdf')
		#bottomup = 0 sets coordinates as top left 0-based...but all the images are upside down
		#Just start at arbitrary high Y value and subtract instead...
		c = canvas.Canvas(pdf_loc)
		c.setTitle("Messaging Analysis")

		#Manually center the header because the documentation on how to do so is...special.
		c.drawString(240, 800, "Messaging Statistics")

		categories = [["Depth","Message Depth"], ["MsgInRate","Message-In Rate"], ["MsgOutRate", "Message-Out Rate"], ["MsgSize", "Message Size"]]
		stat_types = [["avg", "Average"], ["stdDev", "Standard Deviation"], ["min", "Minimum"], ["max", "Maximum"]]
		startY = 750

		for category in categories:
			c.drawString(40, startY, "%s:" % category[1])
			for stat_type in stat_types:
				startY -= 20
				c.drawString(70, startY,"%s %s: %s" % (category[1], stat_type[1], stats["overallStats"]["%s%s"%(stat_type[0],category[0])]))
			startY -= 30
		c.showPage()
		
		images = os.listdir(file_loc)
		for image in images:
			if fnmatch.fnmatch(image, '*graph.jpeg'):
				c.drawImage(os.path.join(file_loc,image), 0, 350, 20*cm, 15*cm)
				c.showPage()
		c.save()
		return pdf_loc


	def create_graphs(self, stats, output_dir, test=False):
		
		self.create_msg_in_rate(stats, output_dir, test)
		self.create_msg_out_rate(stats, output_dir, test)
		self.create_msg_size_histogram(stats, output_dir, test)
		self.create_msg_depth_histogram(stats, output_dir, test)
		self.create_conn_count_graph(stats, output_dir, test)
		self.create_msg_persistence_hist(stats, output_dir, test)
		if(test): plt.show()

		return output_dir
		
	
	def create_msg_in_rate(self, stats, output_dir, test=False):
		
		bar_width = (stats["intervalStats"][0]["startTime"] - stats["intervalStats"][0]["endTime"]) / 10
		x,y = [], []
		for i in range(0,len(stats["intervalStats"])):
			x.append(stats["intervalStats"][i]["startTime"])
			y.append(stats["intervalStats"][i]["msgInRate"])
			
		plt.figure()
		plt.bar(x,y,width=bar_width)
		plt.xlabel('Start Time')
		plt.ylabel('Msg Rate (msgs/second)')
		plt.title('Msg In Rate Over Time')
		plt.grid(True)
		plt.draw()
		plt.savefig(os.path.join(output_dir,"msg_in_rate_graph.jpeg"))
		
		if(test): 
			plt.show()
		plt.close()

	def create_msg_out_rate(self, stats, output_dir, test=False):
		
		bar_width = (stats["intervalStats"][0]["startTime"] - stats["intervalStats"][0]["endTime"]) / 10
		x,y = [], []
		for i in range(0,len(stats["intervalStats"])):
			x.append(stats["intervalStats"][i]["startTime"])
			y.append(stats["intervalStats"][i]["msgOutRate"])
			
		plt.figure()
		plt.bar(x,y,width=bar_width)
		plt.xlabel('Start Time')
		plt.ylabel('Msg Rate (msgs/second)')
		plt.title('Msg Out Rate Over Time')
		plt.grid(True)
		plt.draw()
		plt.savefig(os.path.join(output_dir,"msg_out_rate_graph.jpeg"))

		if(test): 
			plt.show()
		plt.close()

	def create_msg_size_histogram(self, stats, output_dir, test=False):
		
		plt.figure()
		plt.xlabel('Msg Size')
		plt.ylabel('Occurence')
		plt.title('Msg Size Histogram')
		plt.hist(stats["overallStats"]["allMsgSizes"], bins=20)
		plt.grid(True)
		plt.draw()
		plt.savefig(os.path.join(output_dir,"msg_size_hist_graph.jpeg"))

		if(test): 
			plt.show()
		plt.close()

	def create_msg_depth_histogram(self, stats, output_dir, test=False):

		plt.figure()
		plt.xlabel('Msg Depth')
		plt.ylabel('Occurence')
		plt.title('Msg Depth Histogram')
		plt.hist(stats['overallStats']['allDepths'])
		plt.grid(True)
		plt.draw()
		plt.savefig(os.path.join(output_dir,"msg_depth_hist_graph.jpeg"))

		if(test): 
			plt.show()
		plt.close()



	def create_conn_count_graph(self, stats, output_dir, test=False):
		
		bar_width = (stats["intervalStats"][0]["startTime"] - stats["intervalStats"][0]["endTime"]) / 10
		x,y = [], []
		for i in range(0,len(stats["overallStats"]["connStats"])):
			x.append(stats["overallStats"]["connStats"][i]["time"])
			y.append(stats["overallStats"]["connStats"][i]["connectionCount"])
			
		plt.figure()
		plt.bar(x,y,width=bar_width)
		plt.xlabel('Start Time')
		plt.ylabel('Connection Count')
		plt.title('Connection Count Over Time')
		plt.grid(True)
		plt.draw()
		plt.savefig(os.path.join(output_dir,"connection_count_graph.jpeg"))

		if(test): 
			plt.show()
		plt.close()

	def create_msg_persistence_hist(self, stats, output_dir, test=False):

		fig = plt.figure()
		ax = fig.add_subplot(111)
		
		plt.xlabel('Persistent')
		plt.ylabel('Occurence')
		plt.title('Msg Persistence Histogram')
		plt.hist(stats['overallStats']['allPersistent'], bins=2)
		plt.grid(True)
		plt.draw()
		
		labels = [item.get_text() for item in ax.get_xticklabels()]
		labels[1] = 'No'
		labels[len(labels)-2] = 'Yes'
		ax.set_xticklabels(labels)

		plt.savefig(os.path.join(output_dir,"msg_persistence_hist_graph.jpeg"))

		if(test): 
			plt.show()
		plt.close()
