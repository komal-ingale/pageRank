Running MapReduce 
Find PageRank.jar in submitted folder which can be run directly using following command
$ hadoop jar PageRank.jar input output                  ---input and output directory of HDFS

To compile Classes and buld jar 
go to Directory /MapReduce/ and run following instructions
$ mkdir -p build
$ javac -cp /ur/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint 
$ jar -cvf pagerank.jar -C build/ .'
$ hadoop jar pagerank.jar pagerank.Main input output    ---input and output directory of HDFS

Running Spark
$ spark-submit page_rank.py inputFileName ouputFileName   ---inputFileName and ouputFileName are name of files on HDFS 

Extra Credit
Implementation of Inverted Index 
InvertedIndex.jar performs document and word search and created inverted Index file in a directory. To run this, use following command
$ hadoop jar InvertedIndex.jar input output  ---input and output directory of HDFS

Search.jar searches a query in inverted Index output and returns result in sorted order. to run this, use following command
$ hadoop jar Search.jar output output1  search_query  ---note that input directory for search will be output direscory of invertedIndex job 



Hadoop and MapReduce Implementation comparision
* The main hurdle in MapReduce is you need to persist data in file, causing lot of string manipulation operations. while in case of Spark, we can cache or persist objects and thus dealing with objects becomes easier.
* Reading and parsing initial data from a given file was easier in Spark, we can directly filter out empty lines and perform map on valid files to find url and outlinks.
* Finding number of documents was easy in Spark because of readily available finction count(), while in case of MapReduce a separate Map Reduce Job needed to perform with the help of Configuration counter
* While computing page ranks iterating over Spark map Reduce task was much easier, in case of MapReduce, we have to take care of input output directories, file parsing and making it in readable format again for mapper to its tasks
* Sorting was easy job in Spark where takeOrder() function directly gives orderd list with argument to get top n list. In case of MapReduce we have to swap key, value and override custom caomarator to make order decscending.
* Performance of Spark is way to better than MapReduce