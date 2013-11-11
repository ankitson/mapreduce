package util;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 2:12 AM
 * To change this template use File | Settings | File Templates.
 */

import java.util.*;

/**
 * Returns a size k cyclic iterator
 * Each iteration returns a list of the next k items.
 *
 * A standard iterator can be considered a 1-iterator.
 *
 * Further, it is cyclic - it auto wraps around. This also means that
 * hasNext() will always be true.
 *
 * If k is larger than the size of the underlying collection, elements will
 * be repeated within the returned list.
 */
public class KCyclicIterator<E> implements Iterator<List<E>> {
    Iterable<E> iterable;
    Iterator<E> it;
    int k;

    public KCyclicIterator(Iterable<E> iterable, int k) {
        this.iterable = iterable;
        System.out.println("iterable: " + iterable);
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

    public static void main(String[] args) {
        Set<Integer> test = new HashSet<Integer>();
        test.add(1);
        test.add(2);
        test.add(3);

        KCyclicIterator<Integer> kIterator = new KCyclicIterator<Integer>(test, 2);
        for (int i=0;i<5;i++) {
            System.out.println(kIterator.next());
        }
    }
}
