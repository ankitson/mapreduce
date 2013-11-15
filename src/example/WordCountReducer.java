package example;

import jobs.KVContainer;
import jobs.ReducerInterface;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/13/13
 * Time: 10:17 PM
 * To change this template use File | Settings | File Templates.
 */

public class WordCountReducer implements ReducerInterface<String, Integer, String, Integer> {

    public void reduce(String key1, String key2, Integer val1, Integer val2, KVContainer<String,Integer> reducedKV) {
        reducedKV.set(key1, val1+val2);
    }

    public KVContainer<String,Integer> parseRecord(String record, int recordNo) {
        String[] split = record.split(":");
        String outKey = split[0];
        Integer outVal = Integer.parseInt(split[1]);
        return new KVContainer<String,Integer>(outKey,outVal);
    }

    public String KVtoString(KVContainer<String,Integer> outputKV) {
        return outputKV.getKey() + ":" + outputKV.getValue();
    }

    public KVContainer<String,Integer> parseRecord(String record) {
        String[] splits = record.split(":");
        return new KVContainer<String,Integer>(splits[0],Integer.parseInt(splits[1]));
    }
}
