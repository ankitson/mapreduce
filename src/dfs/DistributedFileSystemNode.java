package dfs;

import messages.FileMessage;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;

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
    private String directoryPath = "./tmp/distributedChunks";

    public DistributedFileSystemNode(int port) {
        try {
            this.serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
        isListening = true;
    }

    public void run() {
        File dir = new File(directoryPath);
        if(!dir.exists()) dir.mkdir();
        ObjectInputStream inputStream = null;
        Socket socket = null;
        FileMessage fileMessage = null;
        while(isListening) {
            try {
                socket = serverSocket.accept();
                inputStream = new ObjectInputStream(socket.getInputStream());
                fileMessage = (FileMessage)inputStream.readObject();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            File file = fileMessage.getFile();
            String fileName = file.getName();
            File tagFile = new File(dir,fileName);

            if (!tagFile.exists()) {
                File parentDir = tagFile.getParentFile();

                if (!parentDir.exists()) {
                    parentDir.mkdirs();
                }
                try {
                    tagFile.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    public static void main(String[] args) {
        DistributedFileSystemNode dfsn = new DistributedFileSystemNode(6666);
        Thread thread = new Thread(dfsn);
        thread.run();
    }

}
