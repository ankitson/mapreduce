package slave;

import dfs.Chunk;
import messages.*;

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

    public static final int FS_LISTEN_PORT = 5358;
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
        while (true) {
            try {
                socket = serverSocket.accept();
                System.out.println("file server got request");
                messenger = new SocketMessenger(socket);
                message = messenger.receiveMessage();
                System.out.println("received message");
                if (message instanceof ChunkMessage) {
                    ChunkMessage cm = ((ChunkMessage) message);
                    System.out.println("received chunk message: " + ((ChunkMessage) cm));
                    Chunk chunk = ((ChunkMessage) message).getChunk();
                    System.out.println("chunk: " + chunk);
                    File chunkFile = new File(chunk.getPathOnHost(cm.getHostName()));
                    System.out.println("looking for: " + chunkFile.getCanonicalPath());
                    if (chunkFile.exists()) {
                        System.out.println("found it");
                        messenger.sendMessage(new FileInfoMessage(chunkFile.getName(), chunkFile.length()));
                        messenger.sendFile(chunkFile);
                    } else {
                        System.out.println("not found");
                        messenger.sendMessage(new FileNotFoundMessage());
                    }
                }
                socket.close();
            } catch (IOException e) {
                System.err.println("Slave FS unable to accept: " + e);
            } catch (ClassNotFoundException e) {
                System.err.println("Slave FS received illegal message: " + e);
            }
        }

    }
}
