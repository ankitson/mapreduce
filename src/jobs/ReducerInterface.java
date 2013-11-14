package jobs;

import util.Pair;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 12:14 AM
 * To change this template use File | Settings | File Templates.
 */
public interface ReducerInterface<K extends Serializable & Comparable<K>,
        V extends Serializable> extends Serializable {

    public Pair<K,V> reduce(Pair val1, Pair val2);
}
