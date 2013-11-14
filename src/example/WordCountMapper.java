package example;

import jobs.CombinerInterface;
import jobs.KVContainer;
import jobs.MapperInterface;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/13/13
 * Time: 10:16 PM
 * To change this template use File | Settings | File Templates.
 */
/*public class WordCountMapper implements MapperInterface<String,Integer> {

    public CombinerInterface combiner;

    public WordCountMapper(CombinerInterface combiner) {
        this.combiner = combiner;
        System.out.println("comber inited in wcmap: " + combiner);
    }

    public Pair<String,Integer> map(String record, int lineNo) {
        return new Pair(record,1);
    }

    public CombinerInterface getCombiner() {
        return combiner;
    }

    public String KVToString(Pair<String,Integer> kvPair) {
        return kvPair.getFirst() + ":" + kvPair.getSecond().toString();
    }

    public Pair<String,Integer> KVFromRecord(String record) {
        String[] parsed = record.split(":");

    }
}*/

public class WordCountMapper implements MapperInterface<String,Integer,String,Integer> {

    public CombinerInterface combiner;

    //TESTING IGNORE COMBINER FOR NOW
    public WordCountMapper(CombinerInterface combiner) {
        combiner = null;
    }

    public void map(String inKey, Integer inValue, KVContainer<String,Integer> collector) {
        collector.set(inKey, new Integer(1));
    }

    public KVContainer<String,Integer> parseRecord(String record, int recordNo) {
        return new KVContainer<String,Integer>(record,recordNo);
    }

    public String KVtoString(KVContainer<String,Integer> outKV) {
        return outKV.getKey() + ":" + outKV.getValue();
    }

}
