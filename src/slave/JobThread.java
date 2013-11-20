package slave;

import dfs.Chunk;
import messages.*;
import util.Host;

import java.io.File;
import java.io.IOException;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/13/13
 * Time: 7:54 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class JobThread implements Runnable {
    public synchronized File getFileFromChunk(Chunk chunk, String hostName) {

        //see if file exists locally
        File chunkPathFile = new File(chunk.getPathOnHost(hostName));
        if (chunkPathFile.exists())
            return chunkPathFile;

        //if not, get it from another slave
        File receivedFile = null;
        for (Host host : chunk.getHosts()) {
            try {
                System.out.println("requesting " + host + " for chunk");
                Socket socket = new Socket(host.HOSTNAME, 9793);
                SocketMessenger hostMessenger = new SocketMessenger(socket);
                hostMessenger.sendMessage(new ChunkMessage(chunk, host.HOSTNAME));
                Message message = hostMessenger.receiveMessage();
                if (message instanceof FileInfoMessage) {
                    FileInfoMessage fim = ((FileInfoMessage) message);
                    receivedFile = new File(Chunk.CHUNK_PATH + fim.getFileName());
                    hostMessenger.receiveFile(receivedFile, (int) fim.getFileSize());
                } else if (message instanceof FileNotFoundMessage) {
                    continue;
                } else {
                    System.err.println("Unknown message received in map servicer");
                }
                socket.close();
            } catch (IOException e) {
                System.err.println("IO Exception trying to get file: " + e);
            } catch (ClassNotFoundException e) {
                System.err.println("Class not found trying to get file: " + e);
            }
        }
        return receivedFile;
    }
}
