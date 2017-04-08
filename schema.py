#Create the same schema in pyhon
from pyspark.sql.types import *

# read the file 
with open("test3.txt", "r") as myFile: 
  contents =myFile.read().replace('\', '')

fields = [StructField(fieldName, StringType, True) for fieldName in contents.split(" ")]
schema = StructType(fields)
                                  
