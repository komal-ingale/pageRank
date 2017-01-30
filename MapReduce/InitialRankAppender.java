/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pagerank;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import static pagerank.Main.DELIMETER;
import static pagerank.Main.NO_OF_DOCUMENTS;

/**
 *
 * @author komal
 */
public class InitialRankAppender extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Job initialRankAppenderJob = Job.getInstance(getConf(), " DocumentCount ");
        initialRankAppenderJob.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(initialRankAppenderJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(initialRankAppenderJob, new Path(args[1]));
        //get number of documents by configuration counter 
         long noOfDocs = Long.parseLong(args[2]);
        initialRankAppenderJob.getConfiguration().set(NO_OF_DOCUMENTS, noOfDocs + "");
        initialRankAppenderJob.setMapperClass(InitializeRankMap.class);
        initialRankAppenderJob.setReducerClass(InitializeRankReduce.class);
        initialRankAppenderJob.setOutputKeyClass(Text.class);
        initialRankAppenderJob.setOutputValueClass(Text.class);
        return initialRankAppenderJob.waitForCompletion(true) ? 0 : 1;

    }

    /**
     * Input : title  #####outlink#####outlink
     * Output : title  @@@@@page_rank#####outlink#####outlink
     * 
     */
    public static class InitializeRankMap extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString().trim();

            String[] parts = line.split(DELIMETER, 2);
            Text title = new Text(parts[0].trim());
            // find initial RANK
            double noOfDocs = Double.parseDouble(context.getConfiguration().get(NO_OF_DOCUMENTS));
            double initialPR = 0.0;
            if (noOfDocs != 0) {
                initialPR = 1 / noOfDocs;
            }
            //Append to final output 
            String value = Main.RANK_DELIMETER + initialPR + DELIMETER + parts[1];

            Text textValue = new Text(value);
            context.write(title, textValue);

        }
    }
    /**
     * Input title  @@@@@page_rank#####outlink#####outlink
     * output title  @@@@@page_rank#####outlink#####outlink
     */

    public static class InitializeRankReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text title, Iterable<Text> documents, Context context)
                throws IOException, InterruptedException {
            for (Text document : documents) {
                context.write(title, document);
            }
        }
    }
}
