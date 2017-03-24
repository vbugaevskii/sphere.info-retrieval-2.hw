import pymorphy2


class Normalizer:
    __stemmer = pymorphy2.MorphAnalyzer()
    __cache = dict()

    def get_normal_form(self, s):
        if len(self.__cache) > 10e6:
            self.__cache = dict()
        
        if s not in self.__cache:
            self.__cache[s] = self.__stemmer.parse(s)[0].normal_form
        return self.__cache[s]
