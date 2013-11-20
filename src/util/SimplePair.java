package util;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 10/11/13
 * Time: 1:27 AM
 * To change this template use File | Settings | File Templates.
 */

public class SimplePair<A,B> {

    private A first;
    private B second;

    public SimplePair() {

    }
    public SimplePair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public A getFirst() {
        return first;
    }

    public B getSecond() {
        return second;
    }

    public String toString() {
        return "Pair: (" + first + "," + second + ")";
    }
}