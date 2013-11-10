package dfs;

import util.Host;

import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
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
    private final int SPLIT_SIZE;

    //map from chunk to set of remote files on hosts where chunk resides
    private Map<Integer, Set<File>> chunksToHosts;

    public DistributedFile(File f, Set<Host> nodeSet, int splitSize) {
        FILE_NAME = f.getName();
        SPLIT_SIZE = splitSize;
        chunkAndSend(f, nodeSet);
    }

    private void chunkAndSend(File file, Set<Host> nodes) {
        Iterator<Host> nodesIterator = nodes.iterator();
        if (!nodesIterator.hasNext()) {
            throw new NoSuchElementException("Cant pass empty set of nodes to distribute to");
        }

        Host currentNode = nodesIterator.next();
        int chunkNo = 1;
        int lineCount = 0;
        File fileChunk = null;
        BufferedWriter chunkWriter = null;

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;

            while ((line = br.readLine()) != null) {
                if (lineCount == SPLIT_SIZE || lineCount == 0) {

                    //close old chunk
                    if (chunkWriter != null)
                        chunkWriter.close();

                    //increment chunk no
                    chunkNo++;

                    //open new chunk handles/writers
                    fileChunk = new File(getChunkPath(currentNode, file, chunkNo));
                    chunkWriter = new BufferedWriter(new FileWriter(fileChunk));

                    //create new chunk file/dir if doesnt exist
                    if (!fileChunk.exists()) {
                        File parentDir = fileChunk.getParentFile();
                        if (!parentDir.exists())
                            parentDir.mkdirs();
                        fileChunk.createNewFile();
                    }

                    //reset linecount
                    lineCount = 1;

                    //get next node
                    if (!nodesIterator.hasNext())
                        nodesIterator = nodes.iterator();
                    currentNode = nodesIterator.next();


                }

                chunkWriter.write(line);
                lineCount++;
            }
        }
        catch (FileNotFoundException e) {
            System.err.println("File to chunk not found: " + e);
        }
        catch (IOException e) {
            System.err.println("IOException chunking/sending file: " + e);
        }
    }

    private String getChunkPath(Host host, File file, int chunkNo) {
        return host.REMOTE_CHUNKS_DIR_PATH + file.getName() + "-" + chunkNo;
    }

}
