package jobs;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 12:14 AM
 * To change this template use File | Settings | File Templates.
 */
public interface ReducerInterface<V> extends Serializable {

    public V reduce(V value1, V value2);
}
