package pagerank;

import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author komal
 */
public class Main {

    static final String NO_OF_DOCUMENTS = "number_of_documents";
    static final String TITLE = "<title>";
    static final String END_TITLE = "</title>";
    static final String REGEX = "\\[\\[.*?]\\]";
    static final String DELIMETER = "#####";
    static final String RANK_DELIMETER = "@@@@@";
    static final double D = 0.85;
    static final int NO_OF_TOP_DOCS = 100;
    static final int NO_OF_ITERATIONS = 10;

    public static void main(String[] args) throws Exception {
        ArrayList<String> foldersTobeDeleted = new ArrayList<String>();
        String argsDC[] = new String[]{args[0], args[0] + "_temp"};
        DocumentCount documentCount = new DocumentCount();

        int res = ToolRunner.run(documentCount, argsDC);
        if (res == 0) {
            long counter = documentCount.getNoOfDocs();
            String argsIO[] = new String[]{argsDC[1], args[1] + "_temp", counter + ""};
            foldersTobeDeleted.add(args[1] + "_temp");
            res = ToolRunner.run(new InitialRankAppender(), argsIO);
            if (res == 0) {
                String argsRR[] = new String[]{argsIO[1], args[1]};
                RecursiveRankCalculator recursiveRankCalculator = new RecursiveRankCalculator();
                res = ToolRunner.run(recursiveRankCalculator, argsRR);
                String finalOutPutPath = recursiveRankCalculator.getFinalOutput();
                if (res == 0) {
                    String argsSort[] = new String[]{finalOutPutPath, args[1]};
                    res = ToolRunner.run(new Sorter(), argsSort);
                }
            }
        }
        for (int i = 0; i < NO_OF_ITERATIONS; i++) {
            foldersTobeDeleted.add(args[1] + i);
        }
        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);
        // clean up all unnecessary directory
        for (String s : foldersTobeDeleted) {

            Path output = new Path(s);

            hdfs.delete(output, true);
            if (hdfs.exists(output)) {

                hdfs.delete(output, true);
            }

        }
    }
}
