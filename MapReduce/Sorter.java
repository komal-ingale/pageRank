/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pagerank;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import static pagerank.Main.DELIMETER;
import static pagerank.Main.NO_OF_TOP_DOCS;
import static pagerank.Main.RANK_DELIMETER;

/**
 *
 * @author komal
 */
public class Sorter extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Job rankerJob = Job.getInstance(getConf(), " DocumentCount ");
        rankerJob.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(rankerJob, args[0]);
        FileOutputFormat.setOutputPath(rankerJob, new Path(args[1]));
        rankerJob.setMapperClass(SortMap.class);
        rankerJob.setReducerClass(SortReduce.class);
        rankerJob.setNumReduceTasks(1);
        rankerJob.setSortComparatorClass(DescComparator.class);
        rankerJob.setOutputKeyClass(DoubleWritable.class);
        rankerJob.setOutputValueClass(Text.class);
        return rankerJob.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Input : title @@@@@newRank#####outlink#####outlink
     * Output : pageRank title 
     */
    public static class SortMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString().trim();
            String[] kayValueParts = line.split(RANK_DELIMETER, 2);
            Text title = new Text(kayValueParts[0].trim());
            String[] parts = kayValueParts[1].split(DELIMETER, 2);
            DoubleWritable pageRank = new DoubleWritable(Double.parseDouble(parts[0].trim()));
            context.write(pageRank, title);
        }
    }

    /**
     * Input : pageRank title
     * 
     * output : title pageRank
     */
    public static class SortReduce extends Reducer<DoubleWritable, Text, Text, Text> {

        int topRows = 0;

        @Override
        public void reduce(DoubleWritable pagRank, Iterable<Text> title, Context context)
                throws IOException, InterruptedException {
          //  if (topRows < NO_OF_TOP_DOCS) {
                for (Text t : title) {
                    context.write(t, new Text(pagRank + ""));
                   // topRows++;
                }
            //}
        }

    }

    //Create new comparator to inverse the sorting done by reduce
    public static class DescComparator extends WritableComparator {

        protected DescComparator() {
            super(DoubleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable key1 = (DoubleWritable) w1;
            DoubleWritable key2 = (DoubleWritable) w2;
            return -1 * key1.compareTo(key2);
        }
    }
}
