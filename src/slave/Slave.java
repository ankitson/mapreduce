package slave;

import dfs.Chunk;
import jobs.Job;
import messages.*;
import util.FileUtils;
import util.Host;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
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
    private String MASTER_HOSTNAME = "UNIX1.ANDREW.CMU.EDU";
    private Host masterHost;
    private Socket masterSocket;
    private SocketMessenger masterMessenger;

    private String directoryPath;
    private String hostName;

    public Slave() {
        try {
            hostName = InetAddress.getLocalHost().getHostName().toUpperCase();
            directoryPath = "./tmp/distributedChunks-" + hostName + "/";
        } catch (UnknownHostException e) {
            System.err.println("Unable to create local chunks dir on slave: " + e);
        }
        masterSocket = null;
    }

    public void mainLoop() {

        //connect to master
        try {
            masterSocket = new Socket(MASTER_HOSTNAME, LISTEN_PORT);
            masterHost = new Host(masterSocket);
            masterMessenger = new SocketMessenger(masterSocket);
        } catch (UnknownHostException e) {
            System.err.println("Error when slave trying to connect to master: " + e);
        } catch (IOException e) {
            System.err.println("Error when slave trying to connect to master: " + e);
        }

        while (true) {
            try {
                Message message = masterMessenger.receiveMessage();

                if (message instanceof JobMessage) {
                    Job job = ((JobMessage) message).job;
                    switch (job.jobType) {
                        case MAP:
                            new Thread(new MapJobServicerThread(job, masterMessenger, job.host.HOSTNAME)).start();
                            break;
                        case REDUCE:
                            new Thread(new ReduceJobServicerThread(job, masterMessenger, job.host.HOSTNAME)).start();
                            break;
                        case DUMMY:
                            System.err.println("Slave received dummy job! Ignoring.");
                            break;
                    }
                } else if (message instanceof FileInfoMessage) {
                    FileInfoMessage fim = (FileInfoMessage) message;
                    File receivedFile = new File(Chunk.CHUNK_PATH + fim.getFileName());
                    FileUtils.createFile(receivedFile);
                    masterMessenger.receiveFile(receivedFile, (int) fim.getFileSize());

                    //append the hostname to the chunk dir and copy the file there
                    String fullPath = receivedFile.getCanonicalPath();
                    String parentDir = fullPath.substring(0,fullPath.lastIndexOf("/"));
                    String newDir = parentDir + "-" + hostName + "/";
                    FileUtils.createDirectory(newDir);
                    String fullNewPath = newDir + fim.getFileName();
                    receivedFile.renameTo(new File(fullNewPath));
                } else if (message instanceof HeartBeatMessage) {
                    //System.out.println("Slave received heartbeat");
                    masterMessenger.sendMessage(new HeartBeatMessage());
                } else if (message instanceof HostNameMessage) {
                    hostName = ((HostNameMessage) message).getHostName();
                    System.out.println("Slave received own hostname from server POV: " + hostName);
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
        new Thread(new FileServerThread()).start();
        slave.mainLoop();
    }

}
