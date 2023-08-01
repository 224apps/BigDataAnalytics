from mrjob.job import MRJob
import re

class WordBigramCount(MRJob):

    def mapper(self, _, line):
        regex = re.compile(r"[\w']+")
        words = regex.findall(line.lower())
        for i in range(1, len(words)):
            bigram = f"{words[i-1]},{words[i]}"
            yield bigram, 1

    def reducer(self, bigram, counts):
        yield bigram, sum(counts)

if __name__ == '__main__':
    WordBigramCount.run()