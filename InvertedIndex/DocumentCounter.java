/*
 *Program to calculate no of c=documents
 */

package invertedindex;

import java.io.IOException;

import javax.tools.Tool;

import org.apache.tools.ant.types.Mapper;

/**
 *
 * @author komal
 */
public class DocumentCounter extends Configured implements Tool {

  Job documentCounterjob;

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    documentCounterjob = Job.getInstance(getConf(), " wordcount ");
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
    System.out.println("in getNoOfDocs " + documentCounterjob.getCounters().findCounter(CounterEnum.Counter).getValue());
    return documentCounterjob.getCounters().findCounter(CounterEnum.Counter).getValue();
  }

  /**
   *Input : File line text
   *Output: line text
   */
  public static class InputParserMap extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

      String line = lineText.toString().trim();

      if (!(line == null || line.isEmpty())) {

        context.write(new Text("key"), new Text(line));
      }

    }
  }

  /**
   *Input : Line text  
   *Output : line Text
   *Counter counts the number of lines in a file
   */
  public static class DocumentCountReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text title, Iterable<Text> documents, Context context) throws IOException, InterruptedException {
      for (Text document : documents) {
        if (!document.toString().isEmpty()) {
          context.write(title, document);
          context.getCounter(CounterEnum.Counter).increment(1);
        }
      }
      //IncrementargsDC counter for each link
    }
  }

}
