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
public class FloatCombiner implements CombinerInterface<String,Float> {

    public Pair<String, Float> combine(Pair kv1, Pair kv2) {
        float v1 = (float) kv1.getSecond();
        float v2 = (float) kv2.getSecond();
        return new Pair(kv1.getFirst(),v1 + v2);
    }
}
