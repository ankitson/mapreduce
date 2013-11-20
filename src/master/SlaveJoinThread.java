package master;

import Config.Configuration;
import messages.SocketMessenger;
import util.Host;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/12/13
 * Time: 12:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class SlaveJoinThread implements Runnable {

    private Configuration config;

    private int SLAVE_TIMEOUT; //config //heartbeat recv timeout
    //should be a multiple of heartbeat SEND frequency (or at least greater)

    private int LISTEN_PORT; //config
    private ConcurrentHashMap<Host, SocketMessenger> messengers;

    public SlaveJoinThread(ConcurrentHashMap<Host, SocketMessenger> messengers, Configuration config) {
        this.messengers = messengers;
        this.config = config;
        this.SLAVE_TIMEOUT = config.SLAVE_TIMEOUT;
        this.LISTEN_PORT = config.MASTER_SLAVE_PORT;
    }

    public void run() {
        ServerSocket listen;
        try {
            listen = new ServerSocket(LISTEN_PORT);
            Socket slaveSocket;
            while (true) {
                slaveSocket = listen.accept();
                System.out.println("Slave connected: " + slaveSocket.getInetAddress().getHostName().toUpperCase());
                Host slaveHost = new Host(slaveSocket);

                SocketMessenger slaveMessenger = new SocketMessenger(slaveSocket,SLAVE_TIMEOUT);
                messengers.put(slaveHost, slaveMessenger);
            }
        } catch (IOException e) {
            System.err.println("Error on slave join listener on master");
        }
    }
}
