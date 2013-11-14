package jobs;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 11:29 PM
 * To change this template use File | Settings | File Templates.
 */

//public abstract class MapperInterface<K extends Serializable & Comparable<K>,V extends Serializable> implements Serializable {

//public abstract class MapperInterface implements Serializable { //TESTING
/*public interface MapperInterface<K extends Serializable & Comparable<K>,V extends Serializable> extends Serializable {

    public Pair<K,V> map(String record, int recordNo);
    public CombinerInterface<K,V> getCombiner();

    public Pair<K,V> KVFromRecord(String record);
    public String KVToString(Pair<K,V> kvPair);

}*/


public interface MapperInterface<
        IK extends Serializable & Comparable<IK>,
        IV extends Serializable & Comparable<IV>,
        OK extends Serializable & Comparable<OK>,
        OV extends Serializable & Comparable<OV>> extends Serializable {

    //map input key and val to output key and val
    public void map(IK inputKey, IV inputValue, KVContainer<OK,OV> collector);

    //map record and record no to input key and val
    public KVContainer<IK,IV> parseRecord(String record, int recordNo);

    //map out key and record to writable string
    public String KVtoString(KVContainer<OK,OV> outputKV);



}
