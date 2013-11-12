package dfs;

import messages.FileInfoMessage;
import messages.SocketMessenger;
import util.FileUtils;
import util.Host;
import util.KCyclicIterator;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
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
        SPLIT_SIZE = 5; //read from config //number of lines in each split
        this.messengers = messengers;
        chunksToHosts = new HashMap<Chunk, Set<Host>>();
        chunkAndSend(f, messengers.keySet());

    }

    private void chunkAndSend(File file, Set<Host> slaves) {
        KCyclicIterator<Host> slavesIterator = new KCyclicIterator<Host>(slaves,
                DistributedFileSystemConstants.REPLICATION_FACTOR);

        int chunkNo = 1;
        Chunk currentChunk = new Chunk(file.getName(), chunkNo);
        int lineCount = 0;
        System.out.println("before chunk");
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
                    System.out.println("Sending chunk " + chunkNo);
                    System.out.println("linecount: " + lineCount);

                    //close the written chunk since it needs to be sent
                    currentChunkFileWriter.close();

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
                        System.out.println("Sent chunk " + chunkNo + ": " + FileUtils.print(currentChunkFile));
                        currentChunkHosts.add(slave);
                    }
                    chunksToHosts.put(currentChunk, currentChunkHosts);


                    chunkNo++;
                    currentChunk = new Chunk(file.getName(), chunkNo);
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
                SocketMessenger slaveMessenger;
                currentChunkHosts = new HashSet<Host>();
                for (Host slave: slavesIterator.next()) {
                    slaveMessenger = messengers.get(slave);
                    if (slaveMessenger == null)
                        continue;

                    slaveMessenger.sendMessage(new FileInfoMessage(currentChunkFile.getName(), currentChunkFile.length()));
                    slaveMessenger.sendFile(currentChunkFile);
                    currentChunkHosts.add(slave);
                }
                chunksToHosts.put(currentChunk, currentChunkHosts);
            }
        } catch (FileNotFoundException e) {
            System.err.println("File to chunk not found: " + e);
        } catch (IOException e) {
            System.err.println("IOException chunking/sending file: " + e);
        }
    }
}
