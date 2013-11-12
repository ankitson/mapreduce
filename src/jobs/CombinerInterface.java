package jobs;

import util.Pair;

import java.io.File;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 12:02 AM
 * To change this template use File | Settings | File Templates.
 */
public interface CombinerInterface<V> {

    public File combine(List<Pair<String, V>> keyValueList);
}
