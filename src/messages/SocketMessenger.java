package messages;

import java.io.*;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 1:49 AM
 * To change this template use File | Settings | File Templates.
 */

public class SocketMessenger {

    public Socket socket;
    public ObjectOutputStream objectOutputStream;
    private ObjectInputStream objectInputStream = null;

    private static final int BUFFER_SIZE = 8192;

    public SocketMessenger(Socket socket) throws IOException {
        this.socket = socket;
        objectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
    }

    public SocketMessenger(Socket socket, int timeout) throws IOException {
        this.socket = socket;
        this.socket.setSoTimeout(timeout);
        objectOutputStream = new ObjectOutputStream(this.socket.getOutputStream());
    }

    public synchronized void sendMessage(Message message) throws IOException {
        objectOutputStream.writeObject(message);
        objectOutputStream.flush();
    }

    public synchronized void sendFile(File f) throws IOException {
        OutputStream os = socket.getOutputStream();
        FileInputStream fis = new FileInputStream(f);

        int count;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((count = fis.read(buffer)) > 0) {
            os.write(buffer, 0, count);
        }
        fis.close();
        os.flush();
    }

    public Message receiveMessage() throws IOException, ClassNotFoundException {
        if (objectInputStream == null) {
            objectInputStream = new ObjectInputStream(socket.getInputStream());
        }
        return (Message) objectInputStream.readObject();
    }

    public boolean receiveFile(File f, int length) throws IOException {
        InputStream is = socket.getInputStream();
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(f));

        int bytesRead;
        byte[] buffer = new byte[length];
        bytesRead = is.read(buffer,0,length);
        bos.write(buffer,0,length);
        bos.close();
        return (bytesRead == length);
    }

    public void close() throws IOException {
        objectOutputStream.close();

        if (objectInputStream != null)
            objectInputStream.close();

        return;
    }
}
