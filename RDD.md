# Resilient Distributed Dataset: 
Its the data structure used in spark. It is very fast and reliable data structure. 

### Spark Context: 
Spark Context Object is created by driver program which is responsible for making RDDs resilient and distributed. It creates RDDs. 

### Transforming RDDs: 
- map
- filter 
- flatmap 
- distinct 
- sample 
- union, intersection, subtract, cartesian 

### RDD Actions: 
- collect
- count 
- reduce 
- countByValue
- take 
- top

NOTE: Nothing happens until you call one of actions listed above(these are only few common actions.). 

