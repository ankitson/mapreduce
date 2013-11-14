package example;

import jobs.CombinerInterface;
import util.Pair;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/13/13
 * Time: 10:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordCountCombiner implements CombinerInterface<String,Integer> {

    public Pair<String, Integer> combine(Pair kv1, Pair kv2) {
        int v1 = (int) kv1.getSecond();
        int v2 = (int) kv2.getSecond();
        return new Pair(kv1.getFirst(),v1 + v2);
    }
}
