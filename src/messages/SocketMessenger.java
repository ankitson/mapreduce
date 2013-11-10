package messages;

import util.Host;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 1:49 AM
 * To change this template use File | Settings | File Templates.
 */

public class SocketMessenger {

    private Host host;
    private Socket socket;
    public ObjectOutputStream objectOutputStream;
    private ObjectInputStream objectInputStream = null;

    public SocketMessenger(Host host) throws IOException {
        this.host = host;
        socket = host.getSocket();
        System.out.println("got socket");
        objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
        System.out.println("got objectoutputstream");
    }

    public void sendMessage(Message message) throws IOException {
        objectOutputStream.writeObject(message);
    }

    public Message receiveMessage() throws IOException, ClassNotFoundException {
        if (objectInputStream == null)
            objectInputStream = new ObjectInputStream(socket.getInputStream());
        return (Message) objectInputStream.readObject();

    }
}
