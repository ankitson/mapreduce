package dfs;

import util.Host;

import java.io.*;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 11:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class Chunk implements Serializable {
    public static final String CHUNK_PATH = "./tmp/distributed-chunks/"; //read from config

    String fileName;
    int chunkNo;
    Set<Host> hosts;

    public Chunk(String fileName, int chunkNo, Set<Host> hosts) {
        this.fileName = fileName;
        this.chunkNo = chunkNo;
        this.hosts = hosts;
    }

    public String getPathOnHost(Host host) {
        String chunkDir = CHUNK_PATH.substring(0, CHUNK_PATH.length() -1);
        return chunkDir + "-" + host.HOSTNAME + "/" + fileName + "-" + chunkNo;
    }

    public String getLocalChunkPath() {
        return CHUNK_PATH + fileName + "-" + chunkNo;
    }

    public void setHosts(Set<Host> hosts) {
        this.hosts = hosts;
    }

    public String toString() {
        return "[" + fileName + " : " + chunkNo + "] stored at { " + hosts + " }";
    }

    public static void main(String[] args) throws IOException {
        File file = new File("./bla.txt");
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        bw.write("blargh");
        bw.close();



        String newDirectory = file.getParent() + "unix1.andrew.cmu.edu";
        System.out.println("new dir: " + newDirectory);
        file.renameTo(new File(Paths.get(newDirectory).resolve(file.getName()).toString()));

        Host h = new Host("unix1.andrew.cmu.edu", 14159);
        Chunk c = new Chunk("filename.txt", 20, new HashSet<Host>());
        System.out.println("path: " + c.getPathOnHost(h));
    }
}
