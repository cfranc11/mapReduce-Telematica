from mrjob.job import MRJob
from mrjob.step import MRStep

class procesar(MRJob):
# A -> Numero de peliculas vistas por usuario, valor promedio de la calificacion

    def mapper1(self, _, line):
        user_id,movie_id,rating,genre,date = line.split(',')
        yield user_id, (1, rating)

    def reducer1(self, user_id, values):
        moviesCont = 0
        rating = 0

        for value in values:
            rating+=int(value[1])
            moviesCont += 1

        yield user_id, (moviesCont, rating/moviesCont)

    def steps(self):
        return [
           MRStep(mapper=self.mapper1, reducer=self.reducer1)]

if __name__ == '__main__':
    procesar.run()
