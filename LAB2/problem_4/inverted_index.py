from mrjob.job import MRJob
import re

class InvertedIndex(MRJob):

    def mapper(self, _, line):
        doc_id, text = line.split(": ", 1)
        regex = re.compile(r"\b\w+\b")
        words = regex.findall(text.lower())
        for word in words:
            yield word, doc_id

    def reducer(self, word, doc_ids):
        yield word, list(doc_ids)

if __name__ == '__main__':
    InvertedIndex.run()