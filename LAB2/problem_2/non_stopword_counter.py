from mrjob.job import MRJob
import re


# List of stopwords
stopwords = {'the', 'and', 'of', 'a', 'to', 'in', 'is', 'it'}

class NonStopwordCounter(MRJob):

    def mapper(self, _, line):
        words = re.findall(r'\w+', line.lower())
        for word in words:
            if word not in stopwords:
                yield word, 1

    def reducer(self, word, counts):
        total_count = sum(counts)
        yield word, total_count
        
        

if __name__ == '__main__':
    NonStopwordCounter.run()