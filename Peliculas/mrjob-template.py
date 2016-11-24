from mrjob.job import MRJob
from mrjob.step import MRStep


class procesar(MRJob):

    def mapper1(self, _, line):
        yield k2, v2

    def reducer1(self, k2, values_k2):

	yield k3, v3

    def steps(self):
        return [
           MRStep(mapper=self.mapper1, reducer=self.reducer1)
	]

if __name__ == '__main__':
    procesar.run()
