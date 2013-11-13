package util;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 12:06 AM
 * To change this template use File | Settings | File Templates.
 */

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;

/**
 * Represents a remote or local host as hostname/port
 */
public class Host implements Serializable {

    public final String HOSTNAME;
    public final int PORT;
    public final String CHUNKS_DIRECTORY = "/tmp/distributed-chunks/";
    public String REMOTE_CHUNKS_DIR_PATH;
    private transient Socket socket = null;

    public Host(String hostName, int port) {
        HOSTNAME = hostName;
        PORT = port;
        REMOTE_CHUNKS_DIR_PATH = "//" + HOSTNAME + CHUNKS_DIRECTORY;
    }

    public Host(Socket socket) {
        this.socket = socket;
        HOSTNAME = socket.getInetAddress().getHostName();
        PORT = socket.getPort();
    }

    public Socket getSocket() throws IOException {
        if (socket != null)
            return socket;

        socket = new Socket(HOSTNAME, PORT);
        return socket;
    }

    public String toString() {
        return HOSTNAME + ":" + PORT;
    }

    public boolean equals(Object other) {
        if (!(other instanceof Host))
            return false;

        Host host = (Host) other;
        return (HOSTNAME.equalsIgnoreCase(host.HOSTNAME)); // && (PORT == host.PORT));
    }

    public int hashCode() {
        return HOSTNAME.toLowerCase().hashCode(); //+ PORT;
    }
}
