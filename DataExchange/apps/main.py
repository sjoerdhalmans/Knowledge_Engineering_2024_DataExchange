from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, types
import re
import pandas as pd
pd.DataFrame.iteritems = pd.DataFrame.items
import numpy as np
from datetime import datetime

import warnings
warnings.filterwarnings('ignore')

# Define data paths (My inner software engineer hates this hardcoding, but for a small script I am not making it dynamic)
def paths():
    pathsList = [
        ".\sparkData\mpox-08-19-2022.csv",
        ".\sparkData\mpox-08-20-2022.csv",
        ".\sparkData\mpox-08-21-2022.csv",
        ".\sparkData\mpox-08-22-2022.csv",
        ".\sparkData\mpox-08-23-2022.csv",
        ".\sparkData\mpox-08-24-2022.csv",
        ".\sparkData\mpox-08-25-2022.csv",
        ".\sparkData\mpox-08-26-2022.csv",
        ".\sparkData\mpox-08-27-2022.csv",
        ".\sparkData\mpox-08-29-2022.csv",
        ".\sparkData\mpox-08-30-2022.csv",
        ".\sparkData\mpox-08-31-2022.csv",
        ".\sparkData\mpox-09-01-2022.csv",
        ".\sparkData\mpox-09-02-2022.csv",
        ".\sparkData\mpox-09-03-2022.csv",
    ]
    return pathsList

# Initialize spark
def get_spark_context() -> SparkContext:
    spark_conf = SparkConf().setAppName("Knowledge Engineering")
    
    spark_conf = spark_conf.setMaster("local[*]")

    context = SparkContext.getOrCreate(spark_conf)

    return context

# Clean reddit data by removing special characters and standardizing the date time format.
def redditCleaner(df: DataFrame):
    data = df.copy()
    # data['original_title'] = df['title']
    data['datetime'] = df["year-month-day"] + " " + df["hour-min-sec"]
    data = data.drop(['year-month-day', 'hour-min-sec'], axis=1)
    data['datetime'] = data.datetime.apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    rt_mask = data.title.apply(lambda x: "RT @" in x)

    # standard tweet preprocessing 
    data.title = data.title.str.lower()
    # Remove twitter handlers
    data.title = data.title.apply(lambda x:re.sub('@[^\s]+','',x))
    #remove hashtags
    data.title = data.title.apply(lambda x:re.sub(r'\B#\S+','',x))
    # Remove URLS
    data.title = data.title.apply(lambda x:re.sub(r"http\S+", "", x))
    # Remove all the special characters
    data.title = data.title.apply(lambda x:' '.join(re.findall(r'\w+', x)))
    #remove all single characters
    data.title = data.title.apply(lambda x:re.sub(r'\s+[a-zA-Z]\s+', '', x))
    # Substituting multiple spaces with single space
    data.title = data.title.apply(lambda x:re.sub(r'\s+', ' ', x, flags=re.I))

    return data

# Clean twitter data by removing special characters and standardizing the date time format.
def tweetCleaner(df: DataFrame):
    data = df.copy()
    # data['original_tweet'] = df['tweet']
    data['datetime'] = df["date"] + " " + df["time"]
    data = data.drop(['date', 'time'], axis=1)
    data['datetime'] = data.datetime.apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    rt_mask = data.tweet.apply(lambda x: "RT @" in x)

    # standard tweet preprocessing 
    data.tweet =data.tweet.str.lower()
    #Remove twitter handlers
    data.tweet = data.tweet.apply(lambda x:re.sub('@[^\s]+','',x))
    #remove hashtags
    data.tweet = data.tweet.apply(lambda x:re.sub(r'\B#\S+','',x))
    # Remove URLS
    data.tweet = data.tweet.apply(lambda x:re.sub(r"http\S+", "", x))
    # Remove all the special characters
    data.tweet = data.tweet.apply(lambda x:' '.join(re.findall(r'\w+', x)))
    #remove all single characters
    data.tweet = data.tweet.apply(lambda x:re.sub(r'\s+[a-zA-Z]\s+', '', x))
    # Substituting multiple spaces with single space
    data.tweet = data.tweet.apply(lambda x:re.sub(r'\s+', ' ', x, flags=re.I))

    return data

# Get the basic dataframes (And apply cleaners)
def getDataFrames(context: SparkContext) -> DataFrame:
    spark_session = SparkSession(context)

    mpoxFrames = []

    mpoxPaths = paths()

    redditPoxPath = ".\sparkData\\reddit_monkeypox.csv"
    pd.set_option('display.max_columns', None)
    for path in mpoxPaths:
        df = pd.read_csv(path, dtype=str, keep_default_na=False).drop('Unnamed: 0', axis=1)
        newMpoxFrame = tweetCleaner(df)
        mpoxFrame = spark_session.createDataFrame(newMpoxFrame)
        mpoxFrames.append(mpoxFrame)

    df = pd.read_csv(redditPoxPath, dtype=str, keep_default_na=False)

    newDf = redditCleaner(df)

    redditPox = spark_session.createDataFrame(newDf)

    dataFrames = [mpoxFrames, redditPox]

    return dataFrames

# Execute some sparkSQL queries on the data, where mapping is not possible insert NULL values.
# Certainly not the most optimal code, but I was also just trying out pandas for the first time
# And playing around with the code a little.
if __name__ == '__main__':
    spark_context = get_spark_context()

    dataframes = getDataFrames(spark_context)

    counter = 0

    newFrames = []

    spark_session = SparkSession(spark_context)

    print(dataframes[0][0].printSchema())
    print(dataframes[1].printSchema())

    dataframes[1].createOrReplaceTempView("redditDataSet")
    dataframes[1].printSchema()
    dataframes[0][0].printSchema()
    filtered_reddit_data_frame = spark_session.sql(
        "SELECT id, title, body, datetime as date, comms_num as replies_count, score, 'NULL' as retweets_count,  'NULL' as likes_count, 'NULL' as language"
        + " FROM redditDataSet"
    )
    filtered_reddit_data_frame.show()
    newFrames.append(filtered_reddit_data_frame)

    for dataframe in dataframes[0]:
        dataframe.createOrReplaceTempView("dataSet")
        dataframe.printSchema()
        filtered_data_frame = spark_session.sql(
            "SELECT id, tweet as title, tweet as body, datetime as date, replies_count, 'NULL' as score, retweets_count, likes_count, language"
            + " FROM dataSet"
        )

        newFrames.append(filtered_data_frame)
        counter += 1

    basicSchema = types.StructType([
        types.StructField("id", types.IntegerType()),
        types.StructField("title", types.StringType()),
        types.StructField("body", types.StringType()),
        types.StructField("date", types.StringType()),
        types.StructField("replies_count", types.IntegerType()),
        types.StructField("score", types.IntegerType()),
        types.StructField("retweets_count", types.IntegerType()),
        types.StructField("likes_count", types.IntegerType()),
        types.StructField("language", types.StringType())
    ])

    emp_RDD = spark_session.sparkContext.emptyRDD()

    data = spark_session.createDataFrame(data = emp_RDD,
                             schema = basicSchema)
    # Just for debugging
    counterzero = 0
    for dataFrame in newFrames:
        data = data.union(dataFrame)
        print(dataFrame.count())
        counterzero += dataFrame.count()

    pandasFrame = data.toPandas()

    pandasFrame.to_csv(".\genData\\appendedData2.csv", encoding='utf-8' ,index=False)

    spark_context.stop()