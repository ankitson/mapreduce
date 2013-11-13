package dfs;

import messages.FileInfoMessage;
import messages.SocketMessenger;
import util.FileUtils;
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
    private final int SPLIT_SIZE; //read from config

    private List<Chunk> chunks;

    private Map<Host, SocketMessenger> messengers;

    public DistributedFile(File f, Map<Host,SocketMessenger> messengers) throws IOException {
        FILE_NAME = f.getName();
        SPLIT_SIZE = 5; //read from config //number of lines in each split
        this.messengers = messengers;
        chunks = new LinkedList<Chunk>();
        chunkAndSend(f, new ArrayList<Host>(messengers.keySet()));

    }

    public String toString() {
        return "[DistributedFile: " + FILE_NAME + " -> " + chunks + "]";
    }


    //must use arraylist
    private void chunkAndSend(File file, List<Host> slaves) {
        KCyclicIterator<Host> slavesIterator = new KCyclicIterator<Host>(slaves,
                DistributedFileSystemConstants.REPLICATION_FACTOR);

        int chunkNo = 1;

        Chunk currentChunk = new Chunk(file.getName(), chunkNo, null);

        int lineCount = 0;
        System.out.println("before chunk");
        System.out.println("local chunk path: " + currentChunk.getLocalChunkPath());
        File currentChunkFile = new File(currentChunk.getLocalChunkPath());
        System.out.println("after chunk");

        Set<Host> currentChunkHosts = new HashSet<Host>();
        boolean currentChunkIsEmpty = true;
        try {
            FileUtils.createFile(currentChunkFile);
            System.out.println("before chunkwriter");
            BufferedWriter currentChunkFileWriter = new BufferedWriter(new FileWriter(currentChunkFile));
            System.out.println("before reader");
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                if ( lineCount % SPLIT_SIZE == 0 && lineCount != 0) {
                    //close the written chunk since it needs to be sent
                    currentChunkFileWriter.close();

                    System.out.println("Sending chunk " + chunkNo + ": " + FileUtils.print(currentChunkFile));

                    SocketMessenger slaveMessenger;
                    currentChunkHosts = new HashSet<Host>();

                    for (Host slave : slavesIterator.next()) {
                        slaveMessenger = messengers.get(slave);

                        //TODO: better fault tolerance here?
                        //currently, a chunk will not be replicated on the right number of slaves
                        //if one of them is dead while chunking
                        if (slaveMessenger == null)
                            continue;

                        slaveMessenger.sendMessage(new FileInfoMessage(currentChunkFile.getName(), currentChunkFile.length()));
                        slaveMessenger.sendFile(currentChunkFile);
                        System.out.println("Sent chunk " + chunkNo + " to host: " + slave);
                        currentChunkHosts.add(slave);
                    }
                    currentChunk.setHosts(currentChunkHosts);
                    chunks.add(currentChunk);


                    chunkNo++;
                    currentChunk = new Chunk(file.getName(), chunkNo, null);
                    currentChunkFile = new File(currentChunk.getLocalChunkPath());
                    currentChunkFileWriter = new BufferedWriter(new FileWriter(currentChunkFile));
                    currentChunkIsEmpty = true;
                }
                currentChunkFileWriter.write(line+"\n");
                currentChunkIsEmpty = false;
                lineCount++;
            }
            if (!currentChunkIsEmpty) {
                currentChunkFileWriter.close();
                System.out.println("Sending chunk " + chunkNo + ": " + FileUtils.print(currentChunkFile));
                SocketMessenger slaveMessenger;
                currentChunkHosts = new HashSet<Host>();
                for (Host slave: slavesIterator.next()) {
                    slaveMessenger = messengers.get(slave);
                    if (slaveMessenger == null)
                        continue;

                    slaveMessenger.sendMessage(new FileInfoMessage(currentChunkFile.getName(), currentChunkFile.length()));
                    slaveMessenger.sendFile(currentChunkFile);
                    System.out.println("Sent chunk " + chunkNo + " to host: " + slave);
                    currentChunkHosts.add(slave);
                }
                currentChunk.setHosts(currentChunkHosts);
                chunks.add(currentChunk);
            }
        } catch (FileNotFoundException e) {
            System.err.println("File to chunk not found: " + e);
        } catch (IOException e) {
            System.err.println("IOException chunking/sending file: " + e);
        }
    }
}
