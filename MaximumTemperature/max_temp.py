from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Maximum Temperatures')
sc = SparkContext(conf=conf)

lines = sc.textFile('1800.csv')

def parsed_line(line):
    fields = line.split(',')
    stationId = fields[0]
    tempType = fields[2]
    temp = fields[3]
    return (stationId, tempType, temp)

parsed_lines = lines.map(parsed_line)
maxTempFilterer = parsed_lines.filter(lambda x: "TMAX" in x)
maxTemps = maxTempFilterer.map(lambda x: (x[0], x[2]))
maxTemperature = maxTemps.reduceByKey(lambda x, y: max(x, y))
results = maxTemperature.collect()

for result in results:
	print(result)
