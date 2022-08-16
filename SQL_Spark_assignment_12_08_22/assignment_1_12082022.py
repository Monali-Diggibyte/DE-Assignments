from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder.appName('MySparkPractise').master('local[2]').getOrCreate()
print(spark)

""" Creating Product dataframe """
print('Creating Product details dataframe')
productData = [('Washing Machine', '1648770933000', 20000, 'Samsung', 'India', '0001'),
               ('Refrigerator', '1648770999000', 35000, ' LG', 'null', '0002'),
               ('Air Cooler', '1648770948000', 45000, ' Voltas', 'null', '0003')
               ]

# productSchema = ['Product_Name', 'Issue_Date', 'Price', 'Brand', 'Country', 'Product_Number']
productSchema = StructType([StructField('Product_Name', StringType(), True),
                            StructField('Issue_Date', StringType(), True),
                            StructField('Price', IntegerType(), True),
                            StructField('Brand', StringType(), True),
                            StructField('Country', StringType(), True),
                            StructField('Product_Number', StringType(), True)
                            ])

productDF = spark.createDataFrame(data=productData, schema=productSchema)

productDF.show(truncate=False)
productDF.printSchema()

""" a)	Convert the Issue Date with the timestamp format. 
Example:  Input: 1648770933000 -> Output: 2022-03-31T23:55:33.000+0000 """
print('Converting the Unix Epoch Datetime with the timestamp format')
from pyspark.sql.functions import *
"""
Note that Unix Epoch time doesn’t support a fraction of the second which is represented with SSS.
https://sparkbyexamples.com/pyspark/pyspark-sql-working-with-unix-time-timestamp/
"""
productDF1= productDF.withColumn('Issue_Date_timestamp', from_unixtime(substring(col('Issue_Date'), 1, 10), "yyyy-MM-dd'T'HH:mm:ss[.SSS][ZZZ]"))
#productDF.show(truncate=False)
productDF1.show(truncate=False)

"""b)	Convert timestamp to date type Example: Input: 2022-03-31T23:55:33.000+0000 -> Output: 2022-03-31 """
print('Converting timestamp to date type')
productDF1= productDF1.withColumn('Issue_date_todate', to_date(col('Issue_Date_timestamp')))
# OR
# productDF2= productDF1.withColumn('Issue_date_todate', from_unixtime(col('Issue_Date_timestamp'), 'yyyy-MM-dd'))
#productDF.show(truncate=False)
productDF1.show(truncate=False)

""" c)	Remove the starting extra space in Brand column for LG and Voltas fields """
print('Removing the starting extra space in Brand column')
productDF1= productDF1.withColumn("Brand_without_space",ltrim("Brand"))
productDF1.show(truncate= False)

""" d)	Replace null values with empty values in Country column """
print('Replacing null values with empty values in Country column')
#Replace part of string with another string
from pyspark.sql.functions import regexp_replace

productDF1 = productDF1.withColumn('Country_NoNull', regexp_replace('Country', 'null', ''))
productDF1.show(truncate=False)

""" 2.	creating Source dataframe """
print('creating Source dataframe')
sourceData= [(150711, 123456, 'EN', '456789', '2021-12-27T08:20:29.842+0000', '0001'),
             (150439, 234567, 'UK', '345678', '2021-01-28T08:21:14.645+0000', '0002'),
             (150647, 345678, 'ES', '234567', '2021-12-27T08:22:42.445+0000', '0003')
             ]
#sourceSchema= ['SourceId', 'TransactionNumber', 'Language', 'ModelNumber', 'StartTime', 'ProductNumber']
sourceSchema= StructType([StructField('SourceId', IntegerType(), True),
                          StructField('TransactionNumber', IntegerType(), True),
                          StructField('Language', StringType(), True),
                          StructField('ModelNumber', StringType(), True),
                          StructField('StartTime', StringType(), True),
                          StructField('ProductNumber', StringType(), True)
                         ])

sourceDF= spark.createDataFrame(data= sourceData, schema= sourceSchema)
sourceDF.show(truncate=False)
sourceDF.printSchema()

""" a)	Change the camel case columns to snake case  Example: SourceId: source_id, TransactionNumber: transaction_number """
print('Changing the camel case columns to snake case')
import re

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


#print(camel_to_snake('getHTTPResponseCode'))  # get_http_response_code
#print(camel_to_snake('HTTPResponseCodeXYZ'))  # http_response_code_xyz
#print(camel_to_snake('SourceId'))  #

sourceDF1 = sourceDF.withColumnRenamed('SourceId', camel_to_snake('SourceId')) \
                    .withColumnRenamed('TransactionNumber', camel_to_snake('TransactionNumber')) \
                    .withColumnRenamed('Language', camel_to_snake('Language'))\
                    .withColumnRenamed('ModelNumber', camel_to_snake('ModelNumber')) \
                    .withColumnRenamed('StartTime', camel_to_snake('StartTime')) \
                    .withColumnRenamed('ProductNumber', camel_to_snake('ProductNumber'))

#sourceDF1 = sourceDF.withColumnRenamed('SourceId','source_id',)
sourceDF1.show(truncate= False)

""" b)	Add another column as start_time_ms and convert the values of StartTime to milliseconds """
sourceDF1= sourceDF1.withColumn('start_time_ms', concat(unix_timestamp(to_date(date_format('start_time',"yyyy-MM-dd HH:mm:ss.SSS"))), substring('start_time',21,3)))
"""
Note that Unix Epoch time doesn’t support a fraction of the second which is represented with SSS.
https://sparkbyexamples.com/pyspark/pyspark-sql-working-with-unix-time-timestamp/
"""
sourceDF1.show(truncate= False)

""" 3.	Combine both the tables based on the Product Number •	and get all the fields in return. """
print('Combine both the dataframes based on the Product Number')
joinDF= productDF.join(sourceDF, productDF.Product_Number== sourceDF.ProductNumber, 'FULLOUTER')

joinDF.show(truncate=False)
joinDF.printSchema()

""" •	And get the country as EN """
print('Getting the country as EN')
# Using equals condition
joinDF.filter(joinDF.Language == "EN").show(truncate=False)

joinDF.select('Country').filter(joinDF.Language == "EN").show(truncate=False)
