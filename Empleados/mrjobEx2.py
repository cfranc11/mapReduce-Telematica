# coding=utf-8

from mrjob.job import MRJob
from mrjob.step import MRStep

# A -> Salario promedio por sector economico

class procesar(MRJob):
    def mapper1(self, _, line):
        se,id_employee,salary,year = line.split(',')
        yield se, salary

    def reducer1(self, se, values):
        cont = 0
        prom = 0

        for value in values:
            prom+=int(value)
            cont+= 1

        yield se, (cont, prom/cont)

    def steps(self):
        return [
           MRStep(mapper=self.mapper1, reducer=self.reducer1)]

if __name__ == '__main__':
    procesar.run()
