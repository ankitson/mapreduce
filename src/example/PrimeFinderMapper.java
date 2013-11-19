package example;

import jobs.KVContainer;
import jobs.MapperInterface;
import jobs.ReducerInterface;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/15/13
 * Time: 3:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class PrimeFinderMapper implements MapperInterface<Integer,Integer,Integer,Boolean> {

    public ReducerInterface<Integer,Boolean,Integer,Boolean> combiner;

    public PrimeFinderMapper(ReducerInterface<Integer,Boolean,Integer,Boolean> combiner) {
        this.combiner = combiner;
    }

    public void map(Integer inKey, Integer inValue, KVContainer<Integer,Boolean> collector) {
        collector.set(inValue, isPrime(inValue));
    }

    public boolean isPrime(int n) {
        for(int i=2;i<n;i++) {
            if(n%i==0)
                return false;
        }
        return true;
    }

    public KVContainer<Integer,Integer> parseRecord(String record, int recordNo) {
        return new KVContainer<Integer,Integer>(recordNo, Integer.parseInt(record));
    }

    public String KVtoString(KVContainer<Integer,Boolean> outKV) {
        return outKV.getKey() + ":" + outKV.getValue();
    }

    public ReducerInterface<Integer,Boolean,Integer,Boolean> getCombiner() {
        return combiner;
    }
}
