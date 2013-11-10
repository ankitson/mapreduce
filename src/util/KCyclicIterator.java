package util;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 2:12 AM
 * To change this template use File | Settings | File Templates.
 */

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Takes any iterator, and transforms it to a k-iterator.
 * Each iteration returns a list of the next k items.
 *
 * A standard iterator can be considered a 1-iterator.
 *
 * Further, it is cyclic - it auto wraps around. So there is no need
 * to use the hasNext() method
 *
 * If k is larger than the size of the underlying collection, elements will
 * be repeated
 */
public class KCyclicIterator<E> implements Iterator<List<E>> {
    Iterable<E> iterable;
    Iterator<E> it;
    int k;

    public KCyclicIterator(Iterable<E> iterable, int k) {
        this.iterable = iterable;
        this.it = iterable.iterator();
        this.k = k;

        if (!it.hasNext())
            throw new IllegalArgumentException("Cant cycle over empty collection");
    }

    public boolean hasNext() {
        return true;
    }

    public List<E> next() {
        List<E> nextK = new ArrayList<E>(k);
        while (nextK.size() != k) {
            if (!it.hasNext())
                it = iterable.iterator();

             nextK.add(it.next());
        }

        return nextK;
    }

    public void remove() {
        throw new UnsupportedOperationException("Remove is not supported");
    }
}
