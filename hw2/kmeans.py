#Run this command

# ~/spark/bin/spark-submit --master spark://master:7077 kmeans.py  <input-file> <k> <initialization-mode> > results.txt

# e.g. ~/spark/bin/spark-submit --master spark://master:7077 kmeans.py  meddata2022.csv 6 k-means > results.txt

#Guide about the arguments in "Commands and Parameters.txt"

from __future__ import print_function

import sys

import numpy as np
from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans as km
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession


def parseVector(line):
    return np.array([float(x) for x in line.split(',')])


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: <source> <input-file> <k> <mode>", file=sys.stderr)
        sys.exit(-1)

    k = int(sys.argv[2])
    mode=str(sys.argv[3])
    if mode == "k-means":
        mode = "k-means||"
    if mode!="k-means||" and mode!="random" :
        print("Choose mode: k-means|| or random")
        sys.exit(-1)
    if k==0:
        spark = SparkSession \
        .builder \
        .appName("Find best k") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
        dataframe = spark.read.csv(path="hdfs://master:9000/user/user/"+sys.argv[1], header=False, inferSchema=True)

        assemble=VectorAssembler().setInputCols(dataframe.columns).setOutputCol('features')
        assembled_data=assemble.transform(dataframe)

        evaluator = ClusteringEvaluator(predictionCol='prediction',featuresCol='features', metricName='silhouette',distanceMeasure='squaredEuclidean')
        
        for k in range(3,30,3):
            KMeans_algo=KMeans(featuresCol='features', k=k)
            model=KMeans_algo.fit(assembled_data)
            output=model.transform(assembled_data)
            score=evaluator.evaluate(output)

            print("Number of Clusters: " + str(k))
            print("Silhouette Score:",score)
            print("Final centers: ", end="")
            for center in model.clusterCenters():
                print(str(center), end=" ")
            print()
            lengths = model.summary.clusterSizes
            print("Cluster sizes: " + str(lengths))
            print("SSE: " + str(model.summary.trainingCost))
            print()
        spark.stop()    
    else: 
        sc = SparkContext(appName=(mode + ", k = " + str(k)))
        lines = sc.textFile("hdfs://master:9000/user/user/"+sys.argv[1])
        data = lines.map(parseVector)

        model = km.train(data, k, initializationMode=mode)
        print("Final Center: " + str(model.clusterCenters))
        print("SSE: " + str(model.computeCost(data)))
        sc.stop()