from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('MinimumTemperatures')
sc = SparkContext(conf=conf)

def parsed_lines(line):
	fields = line.split(',')
	stationId = fields[0]
	entryType = fields[2]
	temperature = float(fields[3])
	return (stationId, entryType, temperature)

lines = sc.textFile('1800.csv')
parsed_lines = lines.map(parsed_lines)
minTemps = parsed_lines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))
results = minTemps.collect()

for result in results:
	print(result)

