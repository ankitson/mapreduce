package dfs;

import messages.FileNameMessage;
import messages.SocketMessenger;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Created with IntelliJ IDEA.
 * User: Adi
 * Date: 11/10/13
 * Time: 1:41 AM
 * To change this template use File | Settings | File Templates.
 */
public class DistributedFileSystemNode implements Runnable{

    private ServerSocket serverSocket;
    private boolean isListening;

    // temporary code
    private String directoryPath;

    public DistributedFileSystemNode(int port) {
        try {
            this.serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
        isListening = true;
        try {
            directoryPath = "./tmp/distributedChunks-" + InetAddress.getLocalHost().getHostName() + "/";
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        System.out.println("Node running");
        File dir = new File(directoryPath);
        if(!dir.exists()) {
            System.out.println("chunks dir does not exist, creating");
            dir.mkdirs();
            System.out.println("created chunks dir");
        }
        System.out.println("directory stuff done");
        ObjectInputStream inputStream = null;
        Socket socket = null;
        File file = null;
        SocketMessenger sender = null;

        try {
            socket = serverSocket.accept();
            sender = new SocketMessenger(socket);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


        while(isListening) {
            try {
                String fileName = ((FileNameMessage) sender.receiveMessage()).getFileName();
                file = new File(directoryPath+fileName);
                sender.receiveFile(file);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            /*BufferedReader br = null;
            try {
                br = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            String line = null;
            try {
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }*/

            /*File tagFile = new File(dir,file.getName());

            if (!tagFile.exists()) {
                File parentDir = tagFile.getParentFile();

                if (!parentDir.exists()) {
                    parentDir.mkdirs();
                }
            }

            file.renameTo(tagFile);*/

            /*System.out.println("received file contents: ");
            BufferedReader br;
            try {
                br = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            String line;
            while ((line = br.readLine()) != null) {

            }*/



        }
    }

    public static void main(String[] args) {
        DistributedFileSystemNode dfsn = new DistributedFileSystemNode(6666);
        Thread thread = new Thread(dfsn);
        thread.run();
    }

}
