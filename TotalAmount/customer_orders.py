from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('CustomerSpents')
sc = SparkContext(conf=conf)

def customer_line(line):
    fields = line.split(',')
    cust_id = int(fields[0])
    price = float(fields[2])
    return (cust_id, price)

input = sc.textFile('customer-orders.csv')
lines = input.map(customer_line)
customer_spendings = lines.reduceByKey(lambda x,y: x+y)
customer_spendings = customer_spendings.collect()
for customer in customer_spendings:
    print('{}:{}'.format(customer[0], customer[1]))