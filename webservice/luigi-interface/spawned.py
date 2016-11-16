import luigi
from sys import exit
from time import sleep

class spawnFlop(luigi.Task):
	integer = luigi.IntParameter(default=0)

	def run(self):
		sleep(5)

		print "Hello, World!"

		if self.integer % 2:
			exit()
