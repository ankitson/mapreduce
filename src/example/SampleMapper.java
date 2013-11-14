package example;

import jobs.CombinerInterface;
import jobs.MapperInterface;
import util.Pair;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/13/13
 * Time: 5:35 PM
 * To change this template use File | Settings | File Templates.
 */

public class SampleMapper implements MapperInterface {

    static final long serialVersionUID = 42L;

    public Pair map(String record, int recordNo) {
        Integer theNum = Integer.parseInt(record);
        return new Pair(theNum, theNum * recordNo);
    }

    public CombinerInterface getCombiner() {
        return null;
    }
}