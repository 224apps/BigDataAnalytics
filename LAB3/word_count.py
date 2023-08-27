# from mrjob.job import MRJob
# import re

# class UniqueWordCount(MRJob):
    
#     def mapper(self, _, line):
#         regex =  re.compile(r"\b\w+\b")
#         words = regex.findall(line.lower())
#         for word in words:
#             yield word, 1
            
#     def reducer(self, word, counts):
#         yield word, sum(counts)
        
        
# # if __name__ == '__main__':
# #     UniqueWordCount.run()


from pyspark.sql import SparkSession
import re

# Create a Spark session
spark = SparkSession.builder.appName("UniqueWordCount").getOrCreate()

# Read input text file(s) and split lines into words
lines = spark.read.text("s3://mylab3hw/input.txt").rdd.map(lambda r: r[0])
words = lines.flatMap(lambda line: re.findall(r"\b\w+\b", line.lower()))

# Map words to (word, 1) pairs
word_counts = words.map(lambda word: (word, 1))

# Reduce by key to get word counts
word_counts = word_counts.reduceByKey(lambda a, b: a + b)

# Collect and print results
results = word_counts.collect()
for (word, count) in results:
    print(f"{word}: {count}")

# Stop the Spark session
spark.stop()