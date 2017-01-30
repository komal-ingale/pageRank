
Create Input directory on hadoop 
$ hadoop fs -mkdir /user/input --path for input directory
Add input files to input directory 
$ hadoop fs -put filename /user/input  --filename indicates name of the document to be added in input directory

1.Running DocWordCount.java

Compile the DocWordCount class.

To compile in a package installation of CDH:
$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* DocWordCount.java -d build -Xlint  --if you want to build the jar on hadoop cluster

To compile in a parcel installation of CDH:
$ mkdir -p build
$ javac -cp /opt/cloudera/parcels/CDH/lib/hadoop/*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/* \
     DocWordCount.java -d build -Xlint 
	
Create a JAR file for the DocWordCount application.
$ jar -cvf DocWordCount.jar -C build/ . 

Run DocWordCount jar file
$ hadoop jar DocWordCount.jar org.myorg.DocWordCount /user/input /user/output1  --Input and output path for Hadoop
to see the output run following command
$ hadoop fs -cat /user/output1/*  --output directory of a Hadoop

2.Running TermFrequency.java
Follow the same procedure to compile and build the jar
Run TermFrequency jar file
$ hadoop jar TermFrequency.jar org.myorg.TermFrequency /user/input /user/output2  --Input and output path for Hadoop
to see the output run following command
$ hadoop fs -cat /user/output2/*  --output directory of a Hadoop

3.Running TFIDF.java
follow the same procedure to compile and build the jar
$ hadoop jar TFIDF.jar org.myorg.TFIDF /user/input /user/output3 2 --Input and output path for Hadoop and no of documents as third argument
to see the output run following command
$ hadoop fs -cat /user/output3/*  --output directory of a Hadoop

4.Running Search.java
follow the same procedure to build the jar
$ hadoop jar Search.jar org.myorg.Search /user/output3 /user/output4 yellow hadoop --Input and output path for Hadoop and query string

5.4.Running Rank.java
follow the same procedure to build the jar
$ hadoop jar Rank.jar org.myorg.Rank /user/output4 /user/output5  --Input and output path for Hadoop 
