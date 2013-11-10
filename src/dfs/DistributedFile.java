package dfs;

import messages.FileNameMessage;
import messages.SocketMessenger;
import util.Host;
import util.KCyclicIterator;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 12:03 AM
 * To change this template use File | Settings | File Templates.
 */
public class DistributedFile {

    private final String FILE_NAME;
    private final int SPLIT_SIZE;

    //map from chunk to set of remote files on hosts where chunk resides
    private Map<Integer, Set<File>> chunksToHosts;

    private Map<Host, SocketMessenger> messengers;

    public DistributedFile(File f, Set<Host> nodeSet, int splitSize) throws IOException {
        FILE_NAME = f.getName();
        SPLIT_SIZE = splitSize;
        messengers = new HashMap<Host,SocketMessenger>();
        chunkAndSend(f, nodeSet);
    }

    // reuse sockets instead of reopening each time
    private void chunkAndSend(File file, Set<Host> nodes) throws IOException {

        KCyclicIterator<Host> nodesIterator = new KCyclicIterator<Host>(nodes,
                DistributedFileSystem.REPLICATION_FACTOR);

        int chunkNo = 0;
        int lineCount = 0;
        File fileChunk = null;
        BufferedWriter chunkWriter = null;
        boolean chunkSent = true;

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;

            while ((line = br.readLine()) != null) {
                if (lineCount % SPLIT_SIZE == 0) {
                    System.out.println("sending chunk");

                    //close old chunk
                    if (chunkWriter != null)
                        chunkWriter.close();

                    //send chunk to node
                    if (fileChunk != null) {
                        System.out.println("file chunk not null");
                        List<Host> hosts = nodesIterator.next();
                        System.out.println("hosts to send chunk to: " + hosts);
                        for (Host host : hosts) {
                            if (!messengers.containsKey(host))
                                messengers.put(host, new SocketMessenger(host));

                            System.out.println("sending chunk " + chunkNo + "to host: " + host);
                            messengers.get(host).sendMessage(new FileNameMessage(fileChunk.getName()));
                            messengers.get(host).sendFile(fileChunk);
                        }
                        chunkSent = true;
                    }

                    //increment chunk no
                    chunkNo++;

                    //open new chunk handles/writers
                    fileChunk = new File(getLocalChunkPath(file, chunkNo));

                    //create new chunk file/dir if doesnt exist
                    if (!fileChunk.exists()) {
                        File parentDir = fileChunk.getParentFile();

                        if (!parentDir.exists()) {
                            parentDir.mkdirs();
                        }
                        fileChunk.createNewFile();
                    }
                    chunkWriter = new BufferedWriter(new FileWriter(fileChunk));
                }
                lineCount++;
                chunkWriter.write(line+"\n");
                chunkSent = false;
                System.out.println("at line: " + lineCount);
            }
            br.close();
            if (chunkSent == false) {
                System.out.println("file chunk not null");
                List<Host> hosts = nodesIterator.next();
                System.out.println("hosts to send chunk to: " + hosts);
                for (Host host : hosts) {
                    if (!messengers.containsKey(host))
                        messengers.put(host, new SocketMessenger(host));

                    System.out.println("sending chunk " + chunkNo + "to host: " + host);
                    messengers.get(host).sendMessage(new FileNameMessage(fileChunk.getName()));
                    messengers.get(host).sendFile(fileChunk);
                }
            }
        }
        catch (FileNotFoundException e) {
            System.err.println("File to chunk not found: " + e);
        }
        catch (IOException e) {
            System.err.println("IOException chunking/sending file: " + e);
        }
    }

    private String getLocalChunkPath(File file, int chunkNo) {
        return DistributedFileSystem.LOCAL_CHUNK_PREFIX + file.getName() + "-" + chunkNo;
    }

    public static void main(String[] args) throws IOException {
        /*File fileChunk = new File("/Users/ankit/a/b/c/bla-new-file.txt");
        if (!fileChunk.exists()) {
            File parentDir = fileChunk.getParentFile();
            if (!parentDir.exists())
                parentDir.mkdirs();
            fileChunk.createNewFile();
        }*/

        File file = new File("./blatest.txt");
        BufferedReader br = new BufferedReader(new FileReader(file));


    }



}
