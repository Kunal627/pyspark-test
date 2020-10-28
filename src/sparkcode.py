import pyspark

def spcon():
    sc = pyspark.SparkContext('local[*]')
    return sc

def processfile(sc,srcpath):
    txt = sc.textFile(srcpath)
    python_lines = txt.filter(lambda line: 'python' in line.lower())
    return python_lines

#sc = spcon()
#srcpath = r'C:\Users\chandk10\OneDrive - Medtronic PLC\Work\pyspark-test\sourcedata\testfile.txt'
#linerdd = processfile(sc,srcpath)
#print(linerdd.collect())