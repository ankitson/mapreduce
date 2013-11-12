package dfs;

import messages.SocketMessenger;
import util.Host;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
    public Set<Host> nodes;
    private Map<Host, SocketMessenger> messengers;


    public DistributedFileSystem(File configFile) {
        nodes = new HashSet<Host>();
        Host node1 = new Host("unix1.andrew.cmu.edu", 6666);
        Host node2 = new Host("unix2.andrew.cmu.edu", 6666);
        nodes.add(node1);
        nodes.add(node2);
        initalizeNodeMessengers();
    }

    public static void main(String[] args) throws IOException {
        File testFile = new File("./10lines.txt");
        DistributedFileSystem dfs = new DistributedFileSystem(new File("./dfsConfigFile"));
        DistributedFile df = new DistributedFile(testFile, dfs.nodes, 5, dfs.messengers);
        DistributedFile df2 = new DistributedFile(testFile, dfs.nodes, 2, dfs.messengers);
        dfs.closeMessengerrs();
    }

    private void initalizeNodeMessengers() {
        messengers = new HashMap<Host,SocketMessenger>();
        for (Host node : nodes) {
            try {
                messengers.put(node, new SocketMessenger(node.getSocket()));
            } catch (IOException e) {
                System.err.println("Unable to connect to node " + node + ". Message:  " + e);
            }
        }
    }

    public void closeMessengerrs() throws IOException {
        for (SocketMessenger messenger : messengers.values()) {
            messenger.close();
        }
    }


}
