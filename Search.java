package org.myorg;

/**
Komal Ingale
kingale@uncc.edu
800936676
**/

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Search extends Configured implements Tool {

    private static final String DELIMETER = "#####";
    private static final String QUERY = "query";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Search(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job j1 = Job.getInstance(conf1);
        String query = "";
		//form a query string starting 3rd argument and set as configuration to the job
        for (int i = 2; i < args.length; i++) {
            query += args[i] + " ";
        }
        j1.getConfiguration().set(QUERY, query, "");
        j1.setJarByClass(Search.class);
        j1.setMapperClass(Map.class);
        j1.setReducerClass(Reduce.class);

        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(DoubleWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j1, new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, new Path(args[1]));
        j1.waitForCompletion(true);

        return j1.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String query = context.getConfiguration().get(QUERY);
            String[] queryWords = query.trim().split(" ");
            String line = lineText.toString();
            String[] parts = line.split(DELIMETER);
            String word = parts[0];
			// check if document contains given query and write to context if it contains
            for (int i = 0; i < queryWords.length; i++) {
                if (queryWords[i].equals(word)) {
                    String[] fileParts = parts[1].split("\t");
                    Text fileName = new Text(fileParts[0]);
                    DoubleWritable tfIdf = new DoubleWritable(Double.parseDouble(fileParts[1]));
                    context.write(fileName, tfIdf);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text fileName, Iterable<DoubleWritable> scores, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
			// calculate tfIdf score for a given word for that file
            for (DoubleWritable score : scores) {
                sum += score.get();
            }
            context.write(fileName, new DoubleWritable(sum));
        }
    }
}
