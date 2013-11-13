package jobs;

import util.Pair;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 11:29 PM
 * To change this template use File | Settings | File Templates.
 */

//public abstract class MapperInterface<K extends Serializable,V extends Serializable> implements Serializable {
public abstract class MapperInterface implements Serializable { //TESTING

    //set to null if no combine step
    public CombinerInterface combiner;
    public abstract Pair map(String record, int recordNo);

}
