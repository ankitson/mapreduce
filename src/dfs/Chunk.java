package dfs;

import util.Host;
import util.Pair;

import java.io.*;
import java.nio.file.Paths;
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
    Pair<Integer,Integer> recordRange; //the range of records this chunk contains

    public Chunk(String fileName, int chunkNo, Set<Host> hosts, Pair<Integer,Integer> recordRange) {
        this.fileName = fileName;
        this.chunkNo = chunkNo;
        this.hosts = hosts;
        this.recordRange = recordRange;
    }

    public String getFileName() {
        return fileName;
    }

    public int getChunkNo() {
        return chunkNo;
    }

    public Set<Host> getHosts() {
        return hosts;
    }

    public String getPathOnHost(String hostName) {
        return getPathPrefixOnHost(hostName) + fileName + "-" + chunkNo;
    }

    public static String getPathPrefixOnHost(String hostName) {
        String chunkDir = CHUNK_PATH.substring(0, CHUNK_PATH.length() -1);
        return chunkDir + "-" + hostName.toUpperCase() + "/";
    }

    public String getLocalChunkPath() {
        return CHUNK_PATH + fileName + "-" + chunkNo;
    }

    public void setHosts(Set<Host> hosts) {
        this.hosts = hosts;
    }

    public void setRecordRange(Pair<Integer,Integer> recordRange) {
        this.recordRange = recordRange;
    }

    public Pair<Integer,Integer> getRecordRange() {
        return recordRange;
    }

    public String toString() {

        if (recordRange != null)
            return String.format("[%s: #%d (lines %d-%d replicated on {%s}",fileName,chunkNo,
                recordRange.getFirst(),recordRange.getSecond(),hosts);

        return String.format("[%s: #%d (lines ? replicated on {%s}", fileName, chunkNo, hosts);
    }

    public static void main(String[] args) throws IOException {
        File file = new File("./bla.txt");
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        bw.write("blargh");
        bw.close();

        String newDirectory = file.getParent() + "UNIX1.ANDREW.CMU.EDU";
        System.out.println("new dir: " + newDirectory);
        file.renameTo(new File(Paths.get(newDirectory).resolve(file.getName()).toString()));
    }
}
