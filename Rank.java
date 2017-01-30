package org.myorg;

/**
Komal Ingale
kingale@uncc.edu
800936676
**/
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.Logger;
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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Rank extends Configured implements Tool {

    private static final String DELIMETER = "#####";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Rank(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        Job j1 = Job.getInstance(conf1);
        j1.setJarByClass(Rank.class);
        j1.setMapperClass(Map.class);
        j1.setReducerClass(Reduce.class);

        j1.setMapOutputKeyClass(DoubleWritable.class);
        j1.setMapOutputValueClass(Text.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);
        j1.setSortComparatorClass(DescComparator.class);
        FileInputFormat.addInputPath(j1, new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, new Path(args[1]));
        j1.waitForCompletion(true);

        return j1.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, DoubleWritable,Text> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            String[] parts = line.split("\t");
            Text fileName = new Text(parts[0]);
            DoubleWritable tfIdf = new DoubleWritable( Double.parseDouble(parts[1]));
			//swap the key value so that it will sort by score
            context.write(tfIdf,fileName);

        }
    }

    public static class Reduce extends Reducer< DoubleWritable,Text, Text, DoubleWritable> {

        @Override
        public void reduce(DoubleWritable score, Iterable<Text> fileName, Context context)
                throws IOException, InterruptedException {
            for (Text fname : fileName) {
                 context.write(fname, score);
            }
           
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
