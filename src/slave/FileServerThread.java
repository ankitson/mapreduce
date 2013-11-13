package slave;

import dfs.Chunk;
import messages.*;
import util.Host;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/13/13
 * Time: 1:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class FileServerThread implements Runnable {

    private final int FS_LISTEN_PORT = 5358;
    private ServerSocket serverSocket;

    public FileServerThread() {

    }

    public void run() {
        try {
            serverSocket = new ServerSocket(5358);
        } catch (IOException e) {
            System.err.println("Slave FS server unable to listen on port " + FS_LISTEN_PORT + ": " + e);
        }

        Socket socket;
        SocketMessenger messenger;
        Message message;
        Host selfHost = new Host(serverSocket.getInetAddress().getHostName(), serverSocket.getLocalPort());
        while (true) {
            try {
                socket = serverSocket.accept();
                messenger = new SocketMessenger(socket);
                message = messenger.receiveMessage();
                if (message instanceof ChunkMessage) {
                    Chunk chunk = ((ChunkMessage) message).getChunk();
                    File chunkFile = new File(chunk.getPathOnHost(selfHost));
                    if (chunkFile.exists()) {
                        messenger.sendMessage(new FileInfoMessage(chunkFile.getName(), chunkFile.length()));
                        messenger.sendFile(chunkFile);
                    } else {
                        messenger.sendMessage(new FileNotFoundMessage());
                    }
                }
            } catch (IOException e) {
                System.err.println("Slave FS unable to accept: " + e);
            } catch (ClassNotFoundException e) {
                System.err.println("Slave FS received illegal message: " + e);
            }
        }

    }
}