package util;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 12:06 AM
 * To change this template use File | Settings | File Templates.
 */

import java.io.IOException;
import java.net.Socket;

/**
 * Represents a remote or local host as hostname/port
 */
public class Host {

    public final String HOSTNAME;
    public final int PORT;
    public final String CHUNKS_DIRECTORY = "/tmp/distributed-chunks/";
    public final String REMOTE_CHUNKS_DIR_PATH;
    private Socket socket = null;

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
}
