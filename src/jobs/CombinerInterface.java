package jobs;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 12:02 AM
 * To change this template use File | Settings | File Templates.
 */
//public interface CombinerInterface<V extends Serializable> extends Serializable { TESTING
public interface CombinerInterface extends Serializable {

    //public File combine(List<Pair<String, V>> keyValueList);
    public File combine(ArrayList keyValueList);
}
