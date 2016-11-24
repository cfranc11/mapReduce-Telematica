from mrjob.job import MRJob
from mrjob.step import MRStep

class procesar(MRJob):
# E -> Dia en que peor evaluaron en promedio los usuarios

    def mapper1(self, _, line):
        user_id,movie_id,rating,genre,date = line.split(',')
        yield date, rating

    def reducer1(self, date, values):
        moviesCont = 0
        rating = 0

        for value in values:
            moviesCont += 1
            rating += int(value)

        yield 1, (rating/moviesCont, moviesCont, date)

    def reducer2(self, date, values):
        max_day = min(values)
        print max_day

    def steps(self):
        return [
           MRStep(mapper=self.mapper1, reducer=self.reducer1), MRStep(reducer=self.reducer2)]

if __name__ == '__main__':
    procesar.run()
