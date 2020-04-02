from pyspark import SparkConf, SparkContext 

conf = SparkConf().setMaster('local').setAppName('Word Counter')
sc = SparkContext(conf=conf)

lines = sc.textFile('book.txt')
words = lines.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
	clean_word = word.encode('ascii', 'ignore')
	if(clean_word):
		print(clean_word, count)

