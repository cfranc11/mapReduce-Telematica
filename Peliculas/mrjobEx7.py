from mrjob.job import MRJob
from mrjob.step import MRStep

class procesar(MRJob):
# F -> La mejor y peor pelicula por genero

    def mapper1(self, _, line):
        user_id,movie_id,rating,genre,date = line.split(',')
        yield genre, (movie_id, rating)

    def reducer1(self, genre, values):
        moviesCont = 0
        rating = 0

        for value in values:
            print genre, value
            moviesCont += 1
            rating += int(value[1])

        yield 1, (rating, moviesCont, genre)

    def reducer2(self, genre, values):
        for value in values:
            print value
    def steps(self):
        return [MRStep(mapper=self.mapper1, reducer=self.reducer1), MRStep(reducer=self.reducer2)]

if __name__ == '__main__':
    procesar.run()
