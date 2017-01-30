from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("demo")
sc = SparkContext(conf=conf)

import sys
import re
import sys
from operator import add

distFile = sc.textFile(sys.argv[1])



#distFile = sc.textFile("graph1.txt")
#Function to load data from file
#Input:a line content(String)
#Output: Text between title tag and a list of outLinks enclosed in '[[ ]]'.


def load_data(line):
	if line:
		url = re.findall(r"<title.*?>(.*?)</title>", line)
		if url:
			outlinks = re.findall("\[\[(.*?)\]\]", line)
			return url[0],outlinks





#Funtion to compute Individual contribution of outlinks
#Input: List of outlinks and current rank of the url
#Output: List of Contributions for all outlinks


def computeIndvContribution(url,outlinks, rank):
	num_outlinks = len(outlinks)
	print num_outlinks
	yield (url,0.0)
    	for outlink in outlinks: 
		yield (outlink, rank / num_outlinks)




#filters empty line and loads data




urls = distFile.filter(lambda x: len(x.strip())>0).map(lambda x: load_data(x)).cache()
#find number of documents



noOfDocuments = urls.count()
print noOfDocuments
#keys= urls.map(lambda x:x[0])
#values = urls.flatMap(lambda x :x[1])
#allUrls=union(keys,values).distinct().cache()
#find initial page rank 



ranks = urls.map(lambda (url, rank) : (url, 1.0/noOfDocuments)).cache()
#for 10 iterations find page rank by repetatively calling map and reduce steps



for iteration in xrange(10):
        # Calculates URL contributions to the rank of other URLs.
        contributions = urls.join(ranks).flatMap(lambda (url, (outlinks, rank)):
            computeIndvContribution(url,outlinks, rank))
        # Re-calculates URL ranks based on oulink contributions.
        ranks = contributions.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)





#write to file
#Enforce Reducer
#take top 100 ranks
sc.parallelize(ranks.coalesce(1).takeOrdered(100, key = lambda x: -x[1])).coalesce(1).map(lambda x: x[0]+ str(" ") +str(x[1])).saveAsTextFile(sys.argv[2])










