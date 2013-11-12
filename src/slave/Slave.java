package slave;

import dfs.Chunk;
import dfs.node.DistributedFileSystemNodeThread;
import jobs.Job;
import messages.JobMessage;
import messages.Message;
import messages.SocketMessenger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/12/13
 * Time: 3:39 AM
 * To change this template use File | Settings | File Templates.
 */
public class Slave {

    private int LISTEN_PORT = 6666; //read from config file
    private ServerSocket listenSocket;
    private SocketMessenger masterMessenger;

    private String directoryPath;

    public Slave() {
        try {
            directoryPath = "./tmp/distributedChunks-" + InetAddress.getLocalHost().getHostName() + "/";
        } catch (UnknownHostException e) {
            System.err.println("Unable to create local chunks dir on slave: " + e);
        }


    }

    public void listen() {
        try {
            listenSocket = new ServerSocket(LISTEN_PORT);
            System.out.println("Slave listening on " + LISTEN_PORT);
        } catch (IOException e) {
            System.err.println("Slave unable to listen on port: " + LISTEN_PORT + ": " + e);
            System.exit(1);
        }

        try {
            masterMessenger = new SocketMessenger(listenSocket.accept());
            new Thread(new DistributedFileSystemNodeThread(masterMessenger, Chunk.CHUNK_PATH)).start();
        } catch (IOException e) {
            System.err.println("Slave error when accepting socket connection: " + e);
        }

        while (true) {
            try {
                Message message = masterMessenger.receiveMessage();
                if (message instanceof JobMessage) {
                    Job job = ((JobMessage) message).job;
                    switch (job.jobType) {
                        case MAP:
                            new Thread(new MapJobServicerThread()).start();
                            break;
                        case REDUCE:
                            new Thread(new ReduceJobServicerThread()).start();
                            break;
                        case DUMMY:
                            System.err.println("Slave received dummy job! Ignoring.");
                            break;
                    }
                } else {
                    System.err.println("Slave received illegal message: " + message);
                }
            } catch (IOException e) {
                System.err.println("Slave error when receiving message: " + e);
            } catch (ClassNotFoundException e) {
                System.err.println("Slave received illegal message: " + e);
            }
        }
    }

    public static void main(String[] args) {
        Slave slave = new Slave();
        slave.listen();
    }

}
