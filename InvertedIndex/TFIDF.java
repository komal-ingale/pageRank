package invertedindex;

/**
 * Komal Ingale kingale@uncc.edu 800936676
 *Progam to calculate TFIDF
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TFIDF extends Configured implements Tool {

//   private static final Logger LOG = Logger .getLogger( WordCount.class);
    private static final String DELIMETER = "##&&##";
    private static final String NO_OF_DOCS = "noOfDocs";
    static final String TITLE = "<title>";
    static final String END_TITLE = "</title>";
    static final String DEF_DEL = "@@@@@";

    static final String REGEX = "<text.*?>(.*?)</text>";

    public int run(String[] args) throws Exception {
        //Job 1 to calculate term frequency
        Configuration conf1 = getConf();
        conf1.set("mapred.textoutputformat.separator", DEF_DEL); //Prior to Hadoop 2 (YARN)
        conf1.set("mapreduce.textoutputformat.separator", DEF_DEL);  //Hadoop v2+ (YARN)
        conf1.set("mapreduce.output.textoutputformat.separator", DEF_DEL);
        conf1.set("mapreduce.output.key.field.separator", DEF_DEL);
        conf1.set("mapred.textoutputformat.separatorText", DEF_DEL);
        Job j1 = Job.getInstance(conf1);
        j1.setOutputFormatClass(TextOutputFormat.class);
        j1.setJarByClass(TFIDF.class);
        j1.setMapperClass(Map.class);
        j1.setReducerClass(Reduce.class);

        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(IntWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(IntWritable.class);
        Path outputPath = new Path(args[0] + "/temp");
        FileInputFormat.addInputPath(j1, new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, outputPath);
//        outputPath.getFileSystem(conf1).delete(outputPath);
        j1.waitForCompletion(true);

        //Job 2 to calculate TFIDF
        Configuration conf2 = new Configuration();
        Job j2 = Job.getInstance(conf2);
//        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value./separator", ":::");
        //send no of documents to reducer 2
        j2.getConfiguration().set(NO_OF_DOCS, args[2]);
        j2.setJarByClass(TFIDF.class);
        j2.setMapperClass(Map2.class);
        j2.setReducerClass(Reduce2.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);

        Path outputPath1 = new Path(args[1]);
        FileInputFormat.addInputPath(j2, outputPath);
        FileOutputFormat.setOutputPath(j2, outputPath1);
//        outputPath1.getFileSystem(conf2).delete(outputPath1, true);
        return j2.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 
     *Input File content
     *Output: word DELEMITER title 1
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            if (line != null && !line.isEmpty()) {
                String title = StringUtils.substringBetween(line, TITLE, END_TITLE);

                Pattern pattern = Pattern.compile(REGEX);

                Matcher matcher = pattern.matcher(line);

                if (matcher.find()) {
                    String text = matcher.group(1);
                    text = text.replace("[[", " ");
                    text = text.replace("]]", " ");
                    text = text.replace("&lt", " ");
                    text = text.replace("&gt", " ");

                    if (text != null && !text.isEmpty()) {
                        for (String word : WORD_BOUNDARY.split(text)) {
                            if (word.isEmpty()) {
                                continue;
                            }
                            Text currentWord = new Text();

                            currentWord = new Text(word + DELIMETER + title);

                            context.write(currentWord, one);

                        }
                    }
                }
            }
        }

    }

    /**
     * Input  word DELEMITER title 1
     * Output word WF
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            context.write(word, new DoubleWritable(Math.log10(sum) + 1));
        }
    }

    /*
     * Input : word WF
     * Output: word WF
     */
    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {

        private final static DoubleWritable one = new DoubleWritable(1);

        public void map(LongWritable key, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();

            String parts[] = line.split(DELIMETER);

            Text currentWord = new Text(parts[0]);

            Text value = new Text(parts[1]);

            context.write(currentWord, value);

        }
    }
    /*
     * Input : word WF
     * Output: word TFIDF
     */

    public static class Reduce2 extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        public void reduce(Text word, Iterable<Text> files, Context context)
                throws IOException, InterruptedException {
            double WF = 0;
            ArrayList< String> list = new ArrayList<String>();
            for (Text file : files) {
                list.add(file.toString());
            }

//            try {
                int docNo = context.getConfiguration().getInt(NO_OF_DOCS, 0);
                double IDF = Math.log10(1 + (docNo / list.size()));
                for (String filename : list) {
                    String[] parts = filename.split(DEF_DEL);
                    Text finalText = new Text(word + DELIMETER + parts[0]);
                    // calculate WF
                    WF = Double.valueOf(parts[1]);
                    //calculate TFIDF
                    double tfidf = WF * IDF;
                    context.write(finalText, new DoubleWritable(tfidf));
                }
//            } catch (Exception e) {
//
//            }

        }
    }
}
