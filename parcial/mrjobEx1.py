# coding=utf-8

from mrjob.job import MRJob
from mrjob.step import MRStep

class procesar(MRJob):
# A -> Promedio de monoxido de carbono(co) agrupados por provincia
    def mapper1(self, _, line):
        vec = line.decode('utf-8','ignore').split(';')
        date = vec[0]
        co = vec[1].strip()
        no = vec[2]
        no2 = vec[3]
        o3 = vec[4]
        pm10 = vec[5]
        sh2 = vec[6]
        pm25 = vec[7]
        pst = vec[8]
        so2 = vec[9]
        province = vec[10]
        season = vec[11]

        yield province, co

    def reducer1(self, province, values):
        cont = 0
        prom = 0

        for value in values:
            try:
                v = float(value)
            except Exception as e:
                v = 0
            prom+=v
            cont += 1

        yield province, (cont, prom/cont)

    def steps(self):
        return [
           MRStep(mapper=self.mapper1, reducer=self.reducer1)]

if __name__ == '__main__':
    procesar.run()
