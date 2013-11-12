package jobs;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 11:29 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class MapperInterface<V> implements Serializable {

    //set to null if no combine step
    public CombinerInterface<V> combiner;
    public abstract V map(String record, int recordNo);

}
