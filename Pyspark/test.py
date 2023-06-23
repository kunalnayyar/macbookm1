from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col,struct,when,explode
from pyspark.sql import Row
from pyspark.sql.types import StringType,IntegerType,FloatType,StructType,StructField
spark = SparkSession.builder.appName("pysparktest").getOrCreate()

data = [(11,'Ali',2000),(22,'Anthony',1000)]
schema = StructType([StructField("ID",IntegerType(),True),StructField("Name",StringType(),True),StructField("Salary",IntegerType(),True)])
df = spark.createDataFrame(data =data ,schema =schema)
print(df.show())


joineddf = sourcedf.join(targetdf,(sourcedf.pk1 == targetdf.pk1))

