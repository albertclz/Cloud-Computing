
# coding: utf-8

# In[1]:


from pyspark.context import SparkContext
from pyspark.context import SparkConf
from pyspark.mllib import recommendation
from pyspark.mllib.recommendation import *
conf = SparkConf().setAppName("App")
conf = (conf.setMaster('local[*]').set('spark.executor.memory', '4G').set('spark.driver.memory', '8G').set('spark.driver.maxResultSize', '8G'))
sc = SparkContext(conf=conf)
sc.setCheckpointDir('tmp')
sc


# In[7]:


user_data=sc.textFile('user_artist_data.txt')
artist_data = sc.textFile('artist_data.txt')
alias = sc.textFile('artist_alias.txt')


# In[10]:


def artist(x):
    k = x.rsplit('\t')
    if len(k) != 2:
        return []
    else:
        try:
            return [(int(k[0]), k[1])]
        except:
            return []


# In[11]:


def aliases(x):
    k = x.rsplit('\t')
    if len(k) != 2:
        return []
    else:
        try:
            return [(int(k[0]), int(k[1]))]
        except:
            return []


# In[12]:


artistAlias = alias.flatMap(lambda x: aliases(x)).collectAsMap()
artist_id = dict(artist_data.flatMap(lambda k: artist(k)).collect())


# In[13]:


lookup = sc.broadcast(artistAlias)

def mapper(x):
    userID, artistID, count = map(lambda lineItem: int(lineItem), x.split())
    finalArtistID = lookup .value.get(artistID)
    if ID is None:
        ID = artistID
    return Rating(userID, ID, count)

Data = user_data.map(lambda x: mapper(x))
Data.cache()


# In[14]:


model = ALS.trainImplicit(Data,rank = 10, iterations = 5, lambda_ = 0.01, alpha = 1.0)
recommendation = model.recommendProducts(2093760 , 10)


# In[8]:


print("Top 10 recommended musicians for user 2093760:")
for i in recommendation:
    print ("Top", k, ": %s" % (artist_id.get(i.product)))


# In[9]:


predict = model.predictAll

