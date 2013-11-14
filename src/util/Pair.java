package util;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 10/11/13
 * Time: 1:27 AM
 * To change this template use File | Settings | File Templates.
 */

public class Pair<A extends Serializable & Comparable<A>, B extends Serializable> implements Serializable, Comparable<Pair<A,B>> {

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

    public int compareTo(Pair<A,B> pair) {
        return first.compareTo(pair.getFirst());
    }

    public String toString() {
        return "Pair: (" + first + "," + second + ")";
    }
}