package jobs;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/14/13
 * Time: 6:21 PM
 * To change this template use File | Settings | File Templates.
 */
public interface MapReduceJob {

    public MapperInterface getMapper();
    public ReducerInterface getReducer();

    public String getInputFileName();

}
