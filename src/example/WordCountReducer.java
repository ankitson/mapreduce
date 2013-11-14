package example;

import jobs.ReducerInterface;
import util.Pair;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/14/13
 * Time: 1:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class WordCountReducer implements ReducerInterface {

    public Pair<String, Integer> reduce(Pair kv1, Pair kv2) {
        int v1 = Integer.parseInt( (String) kv1.getSecond());
        int v2 = Integer.parseInt( (String) kv2.getSecond());
        return new Pair(kv1.getFirst(),v1 + v2);
    }
}
