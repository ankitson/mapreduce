package example;

import jobs.MapReduceJob;
import jobs.MapperInterface;
import jobs.ReducerInterface;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/14/13
 * Time: 6:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordCountMapReduceJob implements MapReduceJob {

    public MapperInterface getMapper() {
        return new WordCountMapper(this.getReducer());
    }

    public ReducerInterface getReducer() {
        return new WordCountReducer();
    }

    public String getInputFileName() {
        return "wordcount.txt";
    }
}
