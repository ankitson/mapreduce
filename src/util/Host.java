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
    public final String REMOTE_CHUNKS_DIR_PATH;
    private transient Socket socket = null;

    public Host(String hostName, int port) {
        HOSTNAME = hostName;
        PORT = port;
        REMOTE_CHUNKS_DIR_PATH = "//" + HOSTNAME + CHUNKS_DIRECTORY;
    }

    public Socket getSocket() throws IOException {
        if (socket != null)
            return socket;

        socket = new Socket(HOSTNAME, PORT);
        return socket;
    }

    /*public String toString() {
        return HOSTNAME + ":" + PORT;
    }*/ //TESTING ONLY

    public boolean equals(Object other) {
        if (!(other instanceof Host))
            return false;

        Host host = (Host) other;
        return (HOSTNAME.equals(host.HOSTNAME) && (PORT == host.PORT));
    }

    public int hashCode() {
        System.out.println("hashing host to: " + (HOSTNAME.hashCode() + PORT));
        return HOSTNAME.hashCode() + PORT;
    }
}
