import re
from pyspark import SparkConf, SparkContext

def normalize_word(text):
	return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster('local').setAppName('Sorted Word Count')
sc = SparkContext(conf = conf)

input = sc.textFile('book.txt')
words = input.flatMap(normalize_word)

word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
word_counts_sorted = word_counts.map(lambda x: (x[1], x[0])).sortByKey()

results = word_counts_sorted.collect()

for result in results:
	count = str(result[0])
	word = result[1].encode('ascii', 'ignore')
	if(word):
		print(word, ':\t\t', count)


