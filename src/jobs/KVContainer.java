package jobs;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/14/13
 * Time: 2:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class KVContainer<
        K extends Serializable & Comparable<K>,
        V extends Serializable> implements Serializable, Comparable<KVContainer<K,V>> {
    private K key;
    private V value;

    public KVContainer() {

    }
    public KVContainer(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public void set(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public int compareTo(KVContainer<K,V> other) {
        return key.compareTo(other.getKey());
    }
}
