# coding=utf-8

from mrjob.job import MRJob
from mrjob.step import MRStep

# C -> Numero de SE por Empleado que ha tenido a lo largo de la estadistica

class procesar(MRJob):
    def mapper1(self, _, line):
        se,id_employee,salary,year = line.split(',')
        yield id_employee, se

    def reducer1(self, id_employee, values):
        stack = []
        for value in values:
            stack.append(value)

        yield id_employee, (len(stack) , stack)

    def steps(self):
        return [
           MRStep(mapper=self.mapper1, reducer=self.reducer1)]

if __name__ == '__main__':
    procesar.run()
