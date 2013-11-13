package util;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 10/11/13
 * Time: 1:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class Pair<A extends Serializable,B extends Serializable> implements Serializable {

    private A first;
    private B second;

    public Pair() {

    }
    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }
}