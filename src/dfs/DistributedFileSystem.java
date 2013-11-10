package dfs;

import util.Host;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/9/13
 * Time: 11:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class DistributedFileSystem {

    public static final int REPLICATION_FACTOR = 2;
    public static final int SPLIT_SIZE = 10; //number of lines in each split
    public static final String LOCAL_CHUNK_PREFIX = "./tmp/distributed-chunks/";
    private Set<Host> nodes;


    public DistributedFileSystem(File configFile) {
        Host node1 = new Host("unix1.andrew.cmu.edu", 6666);
        Host node2 = new Host("unix2.andrew.cmu.edu", 6666);
        nodes.add(node1);
        nodes.add(node2);
    }

    public static void main(String[] args) throws IOException {
        File testFile = new File("./10lines.txt");
        DistributedFileSystem dfs = new DistributedFileSystem(new File("./dfsConfigFile"));
        DistributedFile df = new DistributedFile(testFile, dfs.nodes, 5);
    }


}
