package dfs;

import util.Host;

import java.io.File;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/9/13
 * Time: 11:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class DistributedFileSystem {

    private final int REPLICATION_FACTOR;
    private final int SPLIT_SIZE; //number of lines in each split
    private Set<Host> nodes;


    public DistributedFileSystem(File configFile) {
        REPLICATION_FACTOR = 2;
        SPLIT_SIZE = 10;
        Host node1 = new Host("unix1.andrew.cmu.edu", 6666);
        Host node2 = new Host("unix2.andrew.cmu.edu", 6666);
        nodes.add(node1);
        nodes.add(node2);
    }

    public static void main(String[] args) {
        File testFile = new File("./10lines.txt");
        DistributedFileSystem dfs = new DistributedFileSystem(new File("./dfsConfigFile"));
        DistributedFile df = new DistributedFile(testFile, dfs.nodes, 5);
    }


}
