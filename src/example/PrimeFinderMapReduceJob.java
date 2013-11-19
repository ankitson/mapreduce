package example;

import jobs.MapReduceJob;
import jobs.MapperInterface;
import jobs.ReducerInterface;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/15/13
 * Time: 3:53 AM
 * To change this template use File | Settings | File Templates.
 */
public class PrimeFinderMapReduceJob implements MapReduceJob {

    public MapperInterface getMapper() {
        return new PrimeFinderMapper(this.getReducer());
    }

    public ReducerInterface getReducer() {
        return new PrimeFinderReducer();
    }

    public String getInputFileName() {
        return "primes.txt";
    }
}
