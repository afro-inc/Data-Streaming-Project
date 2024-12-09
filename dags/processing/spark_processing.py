import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

#Init
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_streaming")

def new_spark_session(app_name: str):
    """Initialise spark session

    Args:
        app_name (str): Name of the application
    """
    
    try:
        spark = SparkSession.builder\
            .appName(app_name)\
            .config("spark.jars", 
                    "spark-sql-kafka-0-10_2.12-3.5.3.jar, " +
                    "kafka-clients-3.4.1.jar," +
                    "commons-pool2-2.11.1.jar," +
                    "spark-token-provider-kafka-0-10_2.12:3.5.3.jar," +
                    "spark-streaming-kafka-0-10-assembly_2.12:3.5.3.jar," +
                    "/usr/share/java/mysql-connector-j-9.1.0.jar")\
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info('Spark session initialized')
        return spark

    except Exception as e:
        logger.error(f'Init failed. Error: {e}')
        return None
    

def get_data(spark: SparkSession, brokers: str, topic: str):
    """Retrieve data from Kafka topic

    Args:
        spark (SparkSession): Spark session
        brokers (str): Kafka brokers
        topic (str): Kafka topic"""
    try:
        df = spark.readStream.format('kafka')\
           .option('kafka.bootstrap.servers', brokers)\
           .option('subscribe', topic)\
           .load()\
           .selectExpr("CAST(value AS STRING)")
        
        logger.info("Streaming dataframe fetched successfully")

        return df
    
    except Exception as e:
        logger.warning(f"Fetching dataframe failed. Error: {e}")

def transform(df):
    """Transforms the dataframe
    Args:
        df (spark dataframe): the dataframe
    """
    schema = StructType([
        StructField('id',IntegerType(), True),
        StructField('uid',StringType(), True),
        StructField('password',StringType(), True),
        StructField('first_name',StringType(), True),
        StructField('last_name',StringType(), True),
        StructField('username',StringType(), True),
        StructField('email',StringType(), True),
        StructField('avatar',StringType(), True),
        StructField('gender',StringType(), True),
        StructField('phone_number',StringType(), True),
        StructField('social_insurance_number',StringType(), True),
        StructField('date_of_birth',DateType(), True),
        StructField('employment_title',StringType(), True),
        StructField('employment_key_skill',StringType(), True),
        StructField('address_city',StringType(), True),
        StructField('address_street_name',StringType(), True),
        StructField('address_street_adress',StringType(), True),
        StructField('address_zip_code',IntegerType(), True),
        StructField('address_state',StringType(), True),
        StructField('address_country',StringType(), True),
        StructField('credit_card__cc_number',StringType(), True),
        StructField('subscription_plan',StringType(), True),
        StructField('subscription_status',StringType(), True),
        StructField('subscription_payment_method',StringType(), True),
        StructField('subscription_term', StringType(), True)
    ])
    df_transformed = df.selectExpr("CAST(value AS STRING)")\
                        .select(from_json(col("value"), schema).alias("data"))\
                        .select("data.*")
    
    return df_transformed

def init_streaming_to_sink(df, user: str, passwd: str):
    """Initiates streaming to mysql

    Args:
        df (_type_): the transformed dataframe
        user (str): the Database username
        passwd (str): the users password
    """

    logger.info("Initiating streaming process...")
    properties = {
        "user": user,
        "password": passwd,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    query = df.writeStream \
            .foreachBatch(lambda batch_df, batch_id: 
                            batch_df.write.jdbc(url="jdbc:mysql://db:3306/db", table="Users", mode="append", properties=properties)) \
            .outputMode("append") \
            .start()
    
    query.awaitTermination()
def main():
    """Initiates the streaming process
    """
    app_name = "Spark MySQL Integration"
    user = 'root'
    passwd = 'admin'
    topic = 'new_user'
    broker= 'kafka:9093'

    spark = new_spark_session(app_name)

    if spark:
        df = get_data(spark, broker, topic)
        if df:
            df_transformed = transform(df)
            init_streaming_to_sink(df_transformed, user, passwd)

if __name__ == '__main__':
    main()
