import luigi
from sys import exit
from time import sleep

class spawnFlop(luigi.Task):
	project = luigi.Parameter(default=None)
	donor_id = luigi.Parameter(default=None)
	sample_id = luigi.Parameter(default=None)
	pipeline_name = luigi.Parameter(default=None)

	def run(self):
		sleep(5)
		print self.project
		print self.donor_id
		print self.sample_id
		print self.pipeline_name

		if int(self.donor_id) % 2:
			raise Exception("This was a failure.")

if __name__ == '__main__':
    luigi.run()
