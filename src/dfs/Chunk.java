package dfs;

import java.io.Serializable;

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

    public Chunk(String fileName, int chunkNo) {
        this.fileName = fileName;
        this.chunkNo = chunkNo;
    }

    public String getLocalChunkPath() {
        return CHUNK_PATH + fileName + "-" + chunkNo;
    }
}
