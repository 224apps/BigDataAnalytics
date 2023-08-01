from mrjob.job import MRJob
import re

class UniqueWordCount(MRJob):
    
    def mapper(self, _, line):
        regex =  re.compile(r"\b\w+\b")
        words = regex.findall(line.lower())
        for word in words:
            yield word, 1
            
    def reducer(self, word, counts):
        yield word, sum(counts)
        
        
if __name__ == '__main__':
    UniqueWordCount.run()