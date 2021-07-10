#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
import time
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pyspark.sql.types as TP
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.feature import StopWordsRemover, Word2Vec, RegexTokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row
from sparknlp import embeddings
from sparknlp.pretrained import PretrainedPipeline
import pickle
from sklearn import metrics


# In[2]:


class context():
    def get_context():
        sc = SparkContext(appName="PySparkShell")
        spark = SparkSession(sc)
        my_schema = TP.StructType([
          TP.StructField(name= 'id',          dataType= TP.IntegerType(),  nullable= True),
          TP.StructField(name= 'label',       dataType= TP.IntegerType(),  nullable= True),
          TP.StructField(name= 'tweet',       dataType= TP.StringType(),   nullable= True)
            ])
        my_data= spark.read.csv('D:/spark/data', schema= my_schema, header=True)
        stage_1 = RegexTokenizer(inputCol= 'tweet' , outputCol= 'tokens', pattern= '\\W')

        stage_2 = StopWordsRemover(inputCol= 'tokens', outputCol= 'filtered_words')

        stage_3 = Word2Vec(inputCol= 'filtered_words', outputCol= 'vector', vectorSize= 100)

        model = LogisticRegression(featuresCol= 'vector', labelCol= 'label')

        pipeline= Pipeline(stages=[stage_1, stage_2, stage_3, model])
        
        pipelineFit= pipeline.fit(my_data)
        
        return pipelineFit


# In[3]:


context.get_context()


# In[ ]:




