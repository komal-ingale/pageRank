/*
 * Main Program to run all Jobs
 */

package invertedindex;

/**
 *
 * @author komal
 */
public class InvertedIndex {

  public static void main(String[] args) throws Exception {
    String argsDC[] = new String[] { args[0], args[1] + "temp1" };
    DocumentCounter documentCount = new DocumentCounter();

    int res = ToolRunner.run(documentCount, argsDC);
    if (res == 0) {
      long counter = documentCount.getNoOfDocs();
      String argsIO[] = new String[] { argsDC[1], args[1], counter + "" };
      res = ToolRunner.run(new TFIDF(), argsIO);
    }
  }
}
