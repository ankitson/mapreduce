package dfs.node;

import messages.SocketMessenger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

/**
 * Created with IntelliJ IDEA.
 * User: Adi
 * Date: 11/10/13
 * Time: 1:41 AM
 * To change this template use File | Settings | File Templates.
 */
public class DistributedFileSystemNode {

    private ServerSocket listenSocket;
    private boolean isListening;
    private int listenPort;

    //path to store received chunks
    private String directoryPath;

    public DistributedFileSystemNode(int port) {
        listenPort = port;

        isListening = true;
        try {
            directoryPath = "./tmp/distributedChunks-" + InetAddress.getLocalHost().getHostName() + "/";
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public void listen() {
        try {
            this.listenSocket = new ServerSocket(listenPort);
        } catch (IOException e) {
            System.err.println("DFS Node unable to listen: " + e);
        }

        while (true) {
            try {
                SocketMessenger socketMessenger = new SocketMessenger(listenSocket.accept());
                DistributedFileSystemNodeThread dfsnt = new DistributedFileSystemNodeThread(socketMessenger, directoryPath);
                new Thread(dfsnt).start();
            } catch (IOException e) {
                System.err.println("DFS Node unable to accept connection: " + e);
            }
        }
    }

    public static void main(String[] args) {
        DistributedFileSystemNode dfsn = new DistributedFileSystemNode(6666);
        dfsn.listen();
    }

}
