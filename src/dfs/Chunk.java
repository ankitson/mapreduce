package dfs;

import java.io.*;
import java.nio.file.Paths;

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

    public static void main(String[] args) throws IOException {
        File file = new File("./bla.txt");
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        bw.write("blargh");
        bw.close();



        String newDirectory = file.getParent() + "unix1.andrew.cmu.edu";
        System.out.println("new dir: " + newDirectory);
        file.renameTo(new File(Paths.get(newDirectory).resolve(file.getName()).toString()));
    }
}
