package dfs.node;

import messages.FileInfoMessage;
import messages.Message;
import messages.SocketMessenger;
import util.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 7:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class DistributedFileSystemNodeThread implements Runnable {

    private SocketMessenger dfsServer;
    private String chunksDirPrefix;
    public DistributedFileSystemNodeThread(SocketMessenger dfsServer, String chunksDirPrefix) {
        this.dfsServer = dfsServer;
        this.chunksDirPrefix = chunksDirPrefix;
        FileUtils.createDirectory(chunksDirPrefix);
    }

    public void run() {
        Message received;
        while (true) {
            try {
                System.out.println("ready to receive");
                received = dfsServer.receiveMessage();
                System.out.println("received message");
                if (received instanceof FileInfoMessage) {
                    FileInfoMessage fim = (FileInfoMessage) received;
                    System.out.println("received file info message: " + fim);
                    File receivedFile = new File(chunksDirPrefix + fim.getFileName());
                    dfsServer.receiveFile(receivedFile, (int) fim.getFileSize());
                    System.out.println("received file" + FileUtils.print(receivedFile));
                }
            } catch (IOException e) {
                System.err.println("DFS Node Thread IOException: " + e);
                return;
            } catch (ClassNotFoundException e) {
                System.err.println("Illegal message received: " + e);
            }
        }

    }
}
