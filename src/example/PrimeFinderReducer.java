package example;

import jobs.KVContainer;
import jobs.ReducerInterface;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/15/13
 * Time: 3:59 AM
 * To change this template use File | Settings | File Templates.
 */
public class PrimeFinderReducer implements ReducerInterface<Integer,Boolean,Integer,Boolean> {

    public void reduce(Integer key1, Integer key2, Boolean val1, Boolean val2, KVContainer<Integer,Boolean> reducedKV) {
        reducedKV.set(key1, val1);
    }

    public KVContainer<Integer,Boolean> parseRecord(String record, int recordNo) {
        String[] split = record.split(":");
        Integer outKey = Integer.parseInt(split[0]);
        Boolean outVal = Boolean.parseBoolean(split[1]);
        return new KVContainer<Integer,Boolean>(outKey,outVal);
    }

    public String KVtoString(KVContainer<Integer,Boolean> outputKV) {
        return outputKV.getKey() + ":" + outputKV.getValue();
    }

    public KVContainer<Integer,Boolean> parseRecord(String record) {
        String[] splits = record.split(":");
        Integer inKey = Integer.parseInt(splits[0]);
        Boolean inValue = Boolean.parseBoolean(splits[1]);
        return new KVContainer<Integer,Boolean>(inKey,inValue);
    }
}
