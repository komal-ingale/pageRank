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
import static pagerank.Main.D;
import static pagerank.Main.DELIMETER;
import static pagerank.Main.NO_OF_ITERATIONS;
import static pagerank.Main.RANK_DELIMETER;

/**
 *
 * @author komal
 */
public class RecursiveRankCalculator extends Configured implements Tool {

    Path inPath;

    public int run(String[] args) throws Exception {
        inPath = new Path(args[0]);
        Path outPath = null;
        boolean success = false;
        // for Number of iterations, repeate map and reduce job to calculate page rank
        for (int i = 0; i < NO_OF_ITERATIONS; i++) {
            System.out.println("\n\n\n" + " Iteration:" + i + "\n\n\n");
            Job pageRankCalucatorJob = Job.getInstance(getConf(), " DocumentCount ");
            outPath = new Path(args[1] + i);
            pageRankCalucatorJob.setJarByClass(this.getClass());

            FileInputFormat.addInputPath(pageRankCalucatorJob, inPath);
            FileOutputFormat.setOutputPath(pageRankCalucatorJob, outPath);

            pageRankCalucatorJob.setMapperClass(PageRankMap.class);
            pageRankCalucatorJob.setReducerClass(PageRankReduce.class);
            pageRankCalucatorJob.setOutputKeyClass(Text.class);
            pageRankCalucatorJob.setOutputValueClass(Text.class);

            success = pageRankCalucatorJob.waitForCompletion(true);
            inPath = outPath;
        }
        return success ? 0 : 1;
    }

    String getFinalOutput() {
        return inPath.getName();
    }

    /**
     * Input : title  @@@@@page_rank#####outlink#####outlink
     * Output : title page_rank
     *          title #####outlink#####outlink
     */
    public static class PageRankMap extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString().trim();

            String[] kayValueParts = line.split(RANK_DELIMETER, 2);
            Text title = new Text(kayValueParts[0].trim());

            String[] parts = kayValueParts[1].split(DELIMETER);
            double currentPageRank = Double.parseDouble(parts[0]);
            double noOfOutLinks = parts.length - 1;
            //calculate contribution for each outlink
            double newPageRank = currentPageRank / noOfOutLinks;
            for (int i = 1; i < parts.length; i++) {
                Text url = new Text(parts[i]);
                context.write(url, new Text(newPageRank + ""));
            }

            //write outlinks 
            String[] outLinks = kayValueParts[1].split(DELIMETER, 2);
            Text links;
            if (outLinks.length == 2) {
                links = new Text(DELIMETER + kayValueParts[1].split(DELIMETER, 2)[1]);
            } else {
                links = new Text("");
            }
            context.write(title, links);
        }
    }
    
    /**
     *Input : title page_rank
     *        title #####outlink#####outlink
     *output :title @@@@@newRank#####outlink#####outlink 
     */

    public static class PageRankReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text title, Iterable<Text> pagRanks, Context context)
                throws IOException, InterruptedException {
            String outLinks = "";
            double newPageRank = 0;
            for (Text pageRank : pagRanks) {

                String pageRankValue = pageRank.toString();
                if (!pageRankValue.isEmpty()) {
                    // if outlinks forward
                    if (pageRankValue.startsWith(DELIMETER)) {
                        outLinks = pageRankValue;
                        //Add all contributions
                    } else {
                        newPageRank += Double.parseDouble(pageRankValue);
                    }
                }
            }
            //calculate page rank with damping factor
            double rankWithDamFactor = (1 - D) + D * newPageRank;
            context.write(title, new Text(RANK_DELIMETER + rankWithDamFactor + outLinks));
        }

    }

}
