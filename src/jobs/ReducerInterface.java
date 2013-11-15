package jobs;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 12:14 AM
 * To change this template use File | Settings | File Templates.
 */
public interface ReducerInterface
        <IK extends Serializable & Comparable<IK>,
         IV extends Serializable,
         OK extends Serializable & Comparable<OK>,
         OV extends Serializable> extends Serializable {

    public void reduce(IK key1, IK key2, IV val1, IV val2, KVContainer<OK,OV> reducedKV);

    //map record and record no to input key and val
    public KVContainer<IK,IV> parseRecord(String record);

    //map out key and record to writable string
    public String KVtoString(KVContainer<OK,OV> outputKV);
}
