package dfs;

import messages.FileInfoMessage;
import messages.SocketMessenger;
import util.FileUtils;
import util.Host;
import util.KCyclicIterator;
import util.Pair;

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

    private static KCyclicIterator<Host> slavesIterator = null;

    public DistributedFile(File f, int splitSize, Map<Host,SocketMessenger> messengers, int replicationFactor) throws IOException {
        FILE_NAME = f.getName();
        SPLIT_SIZE = splitSize; //read from config //number of lines in each split
        this.messengers = messengers;
        chunks = new LinkedList<Chunk>();

        ArrayList<Host> slaves = new ArrayList<Host>(messengers.keySet());
        int replicateSize = Math.min(replicationFactor, slaves.size());
        List<Host> slavesToReplicateOn = slaves.subList(0, replicateSize);

        chunkAndSend(f, slavesToReplicateOn);
    }

    public String toString() {
        return "[DistributedFile: " + FILE_NAME + " -> " + chunks + "]";
    }

    public List<Chunk> getChunks() {
        return chunks;
    }


    //must use arraylist
    private void chunkAndSend(File file, List<Host> slaves) {
        System.out.println("Distributing file: " + file.getName());
        if (slavesIterator == null)
            slavesIterator = new KCyclicIterator<Host>(slaves,DistributedFileSystemConstants.REPLICATION_FACTOR);

        int chunkNo = 1;

        Chunk currentChunk = new Chunk(file.getName(), chunkNo, null, new Pair<Integer,Integer>());

        int prevChunkEnd = 0;
        int lineCount = 0;
        File currentChunkFile = new File(currentChunk.getLocalChunkPath());

        Set<Host> currentChunkHosts = new HashSet<Host>();
        boolean currentChunkIsEmpty = true;
        try {
            FileUtils.createFile(currentChunkFile);
            BufferedWriter currentChunkFileWriter = new BufferedWriter(new FileWriter(currentChunkFile));
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                if ( lineCount % SPLIT_SIZE == 0 && lineCount != 0) {
                    //close the written chunk since it needs to be sent
                    currentChunkFileWriter.close();

                    SocketMessenger slaveMessenger;
                    currentChunkHosts = new HashSet<Host>();

                    for (Host slave : slavesIterator.next()) {
                        slaveMessenger = messengers.get(slave);

                        //better fault tolerance here?
                        //currently, a chunk will not be replicated on the right number of slaves
                        //if one of them is dead while chunking
                        if (slaveMessenger == null) {
                            System.out.println("messenger was null during file distribution: " + slaveMessenger);
                            continue;
                        }

                        slaveMessenger.sendMessage(new FileInfoMessage(currentChunkFile.getName(), currentChunkFile.length()));
                        slaveMessenger.sendFile(currentChunkFile);
                        currentChunkHosts.add(slave);
                    }
                    currentChunk.setHosts(currentChunkHosts);
                    currentChunk.setRecordRange(new Pair<Integer, Integer>(prevChunkEnd + 1, lineCount));
                    prevChunkEnd = lineCount;
                    chunks.add(currentChunk);


                    chunkNo++;
                    currentChunk = new Chunk(file.getName(), chunkNo, null, new Pair<Integer,Integer>());
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
                currentChunk.setHosts(currentChunkHosts);
                currentChunk.setRecordRange(new Pair<Integer,Integer>(prevChunkEnd+1,lineCount));
                chunks.add(currentChunk);
            }
        } catch (FileNotFoundException e) {
            System.err.println("File to chunk not found: " + e);
        } catch (IOException e) {
            System.err.println("IOException chunking/sending file: " + e);
        }
    }
}
