from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields=line.split(',')
    stationID=fields[0]
    entryType=fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    ## split & parse logic temp F -> C
        #map value
    ##
    return (stationID, entryType, temperature)

### station_id, , type, fahrenheit, ...
lines = sc.textFile("file:///opt/bitnami/spark/datasets/1800.csv")
parsedLines = lines.map(parseLine)
# for result in parsedLines.collect():
#     print(result)
# exit(1)

# Filter only TMIN
minTemps=parsedLines.filter(lambda x: x[1] == "TMIN")
# for result in minTemps.collect():
#     print(result)
# exit(1)

# Map for tuple / Mapvalues for (a,b)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# for result in stationTemps.collect():
#     print(result[0] + "\t{:.2f}F".format(result[1]))
# exit(1)

minTemps = stationTemps.reduceByKey(
    lambda x, y: min(x, y))

for result in minTemps.collect():
    print(result[0] + "\t{:.2f}F".format(result[1]))

#reduceByKey - Find min()

