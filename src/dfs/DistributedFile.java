package dfs;

import messages.FileInfoMessage;
import messages.SocketMessenger;
import util.FileUtils;
import util.Host;
import util.KCyclicIterator;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 12:03 AM
 * To change this template use File | Settings | File Templates.
 */
public class DistributedFile {

    private final String FILE_NAME;
    private final int SPLIT_SIZE; //read from config

    private Map<Chunk,Set<Host>> chunksToHosts;

    private Map<Host, SocketMessenger> messengers;

    public DistributedFile(File f, Map<Host,SocketMessenger> messengers) throws IOException {
        FILE_NAME = f.getName();
        SPLIT_SIZE = 10; //read from config
        this.messengers = messengers;
        chunkAndSend(f, messengers.keySet());
    }

    private void chunkAndSend(File file, Set<Host> nodes) throws IOException {

        KCyclicIterator<Host> nodesIterator = new KCyclicIterator<Host>(nodes,
                DistributedFileSystemConstants.REPLICATION_FACTOR);

        int chunkNo = 0;
        int lineCount = 0;
        File fileChunk = null;
        BufferedWriter chunkWriter = null;
        boolean chunkSent = true;
        Chunk chunk = null;
        Set<Host> currentChunkHosts;

        try {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;

            while ((line = br.readLine()) != null) {
                if (lineCount % SPLIT_SIZE == 0) {
                    System.out.println("sending chunk");


                    chunk = new Chunk(file.getName(), chunkNo);

                    //close old chunk
                    if (chunkWriter != null)
                        chunkWriter.close();

                    //send chunk to node
                    if (fileChunk != null) {
                        System.out.println("file chunk not null");
                        List<Host> hosts = nodesIterator.next();
                        System.out.println("hosts to send chunk to: " + hosts);
                        currentChunkHosts = new HashSet<Host>();
                        for (Host host : hosts) {
                            if (!messengers.containsKey(host)) {
                                System.err.println("No connection to host " + host + ", skipping.");
                                continue;
                            }
                            currentChunkHosts.add(host);

                            System.out.println("sending chunk " + chunkNo + "to host: " + host);
                            messengers.get(host).sendMessage(new FileInfoMessage(fileChunk.getName(), fileChunk.length()));
                            System.out.println("sent file name message: " + fileChunk.getName() + "," + fileChunk.length());
                            messengers.get(host).sendFile(fileChunk);
                            System.out.println("sent file: " + FileUtils.print(fileChunk));
                        }
                        chunksToHosts.put(chunk, currentChunkHosts);
                        chunkSent = true;
                    }

                    //increment chunk no
                    chunkNo++;

                    //open new chunk handles/writers
                    fileChunk = new File(chunk.getLocalChunkPath());

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
                chunkWriter.close();
                System.out.println("file chunk not null");
                List<Host> hosts = nodesIterator.next();
                System.out.println("hosts to send chunk to: " + hosts);
                for (Host host : hosts) {
                    if (!messengers.containsKey(host)) {
                        System.err.println("No connection to host " + host + ", skipping.");
                        continue;
                    }

                    System.out.println("sending chunk " + chunkNo + "to host: " + host);
                    messengers.get(host).sendMessage(new FileInfoMessage(fileChunk.getName(), fileChunk.length()));
                    System.out.println("sent file name message: " + fileChunk.getName() + "," + fileChunk.length());
                    messengers.get(host).sendFile(fileChunk);
                    System.out.println("sent file: " + FileUtils.print(fileChunk));
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
