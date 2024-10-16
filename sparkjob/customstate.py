from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import GroupState
from utils import gcp,statehandler

def update_state(keys, batch_df, state: GroupState):
    pass


if __name__ == "__main__":
    args = gcp.parse_args()
    spark = SparkSession.builder.appName("spark project on gcp").getOrCreate()



df = spark.readStream.format("pubsub")