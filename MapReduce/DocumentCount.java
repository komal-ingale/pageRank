/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pagerank;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.WordCount;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import static pagerank.Main.DELIMETER;
import static pagerank.Main.END_TITLE;
import static pagerank.Main.REGEX;
import static pagerank.Main.TITLE;

/**
 *
 * @author komal
 */
public class DocumentCount extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(WordCount.class);
    Job documentCounterjob;

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //Job for counting number of documents
        documentCounterjob = Job.getInstance(conf, " DocumentCount ");
        documentCounterjob.setJarByClass(this.getClass());

        FileInputFormat.addInputPaths(documentCounterjob, args[0]);
        Path intermedialOutputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(documentCounterjob, intermedialOutputPath);
        documentCounterjob.setMapperClass(InputParserMap.class);
        documentCounterjob.setReducerClass(DocumentCountReduce.class);
        documentCounterjob.setNumReduceTasks(1);
        documentCounterjob.setOutputKeyClass(Text.class);
        documentCounterjob.setOutputValueClass(Text.class);
        intermedialOutputPath.getFileSystem(documentCounterjob.getConfiguration()).delete(intermedialOutputPath);

        return documentCounterjob.waitForCompletion(true) ? 0 : 1;

    }

    long getNoOfDocs() throws IOException {
        return documentCounterjob.getCounters().findCounter(CounterEnum.Counter).getValue();
    }

    /**
     * Input: document line
     * Output : title  #####outlink#####outlink
     */
    public static class InputParserMap extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString().trim();

            if (!(line == null || line.isEmpty())) {

                String title = StringUtils.substringBetween(line, TITLE, END_TITLE);

                Pattern pattern = Pattern.compile(REGEX);

                Matcher matcher = pattern.matcher(line);

                StringBuilder stringBuilder = new StringBuilder();

                while (matcher.find()) { // loop on each outgoing link
                    String url = matcher.group().replace("[[", "").replace("]]", ""); // drop the brackets and any nested ones if any
                    if (!url.isEmpty()) {
                        stringBuilder.append(DELIMETER + url);
                        //Count all outlinks 
//                        context.write(new Text(url), new Text(""));
                    }
                }
                //count all urls specified in <title></title>
                context.write(new Text(title), new Text(stringBuilder.toString()));
            }

        }
    }

    /**
     * Input : title  #####outlink#####outlink
     * output : title  #####outlink#####outlink
     * find count
     */
    public static class DocumentCountReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text title, Iterable<Text> documents, Context context)
                throws IOException, InterruptedException {
            for (Text document : documents) {
                if (!document.toString().isEmpty()) {
                    context.write(title, document);
                }
            }
            //Increment counter for each link
            context.getCounter(CounterEnum.Counter).increment(1);
        }
    }

    /**
     *
     */
}
