from pyspark.sql.types import *
from pyspark.sql.functions import *

class IOConfig:
    def __init__(self):
        self.pubsubschema = StructType([
                StructField("ss", StringType(), True),      # ss as string
                StructField("dataval", StringType(), True), # dataval as string
                StructField("value_0", IntegerType(), True),# value_0 as integer
                StructField("value_1", IntegerType(), True),# value_1 as integer
                StructField("apikey", StringType(), True)   # apikey as string
            ])
        self.stateschema = StructType([
                            StructField("ss", StringType(), True),  # ss as string
                            StructField("dataval", StructType([      # dataval as a nested structure
                                StructField("value_0", IntegerType(), True),  # value_0 as integer
                                StructField("value_1", IntegerType(), True)   # value_1 as integer
                            ]), True),
                            StructField("apikey", StringType(), True)  # apikey as string
                        ])
        self.outputschema = StructType([
                    StructField("ss", StringType(), True),  # ss as string
                    StructField("dataval", StructType([      # dataval as a nested structure
                        StructField("value_0", IntegerType(), True),  # value_0 as integer
                        StructField("value_1", IntegerType(), True)   # value_1 as integer
                    ]), True),
                    StructField("apikey", StringType(), True)  # apikey as string
                ])
        
        self.statecols = ["ss","dataval","apikey"]

    def getstatecols(self):
        return self.statecols
    
    def getpubsubschema(self):
        return self.pubsubschema
    
    def getstateschema(self):
        return self.stateschema
    
    def getoutputschema(self):
        return self.outputschema
