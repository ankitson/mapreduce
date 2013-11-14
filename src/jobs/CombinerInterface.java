package jobs;

import util.Pair;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 12:02 AM
 * To change this template use File | Settings | File Templates.
 */
//public interface CombinerInterface<V extends Serializable> extends Serializable { TESTING


//public interface CombinerInterface extends Serializable {

public interface CombinerInterface<K extends Serializable & Comparable<K>,
        V extends Serializable> extends Serializable {

    public Pair<K,V> combine(Pair val1, Pair val2);
}
