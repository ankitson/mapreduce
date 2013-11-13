package slave;

import dfs.Chunk;
import dfs.node.DistributedFileSystemNodeThread;
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
    private String MASTER_HOSTNAME = "unix1.andrew.cmu.edu";
    private Host masterHost;
    private Socket masterSocket;
    private SocketMessenger masterMessenger;

    private String directoryPath;
    private String hostName;

    public Slave() {
        try {
            hostName = InetAddress.getLocalHost().getHostName();
            directoryPath = "./tmp/distributedChunks-" + InetAddress.getLocalHost().getHostName() + "/";
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

        //start dfs
        new Thread(new DistributedFileSystemNodeThread(masterMessenger, Chunk.CHUNK_PATH)).start();

        while (true) {
            try {
                Message message = masterMessenger.receiveMessage();
                System.out.println("Slave received message: " + message);
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
                } else if (message instanceof FileInfoMessage) {
                    FileInfoMessage fim = (FileInfoMessage) message;
                    System.out.println("received file info message: " + fim);
                    File receivedFile = new File(Chunk.CHUNK_PATH + fim.getFileName());
                    masterMessenger.receiveFile(receivedFile, (int) fim.getFileSize());
                    System.out.println("received file" + FileUtils.print(receivedFile));

                    //append the hostname to the chunk dir and copy the file there
                    String fullPath = receivedFile.getCanonicalPath();
                    String parentDir = fullPath.substring(0,fullPath.lastIndexOf("/"));
                    System.out.println("parent dir: " + parentDir);
                    String newDir = parentDir + "-" + hostName + "/";
                    FileUtils.createDirectory(newDir);
                    String fullNewPath = newDir + fim.getFileName();
                    System.out.println("full new path: " + fullNewPath);
                    System.out.println("rename: " + receivedFile.renameTo(new File(fullNewPath)));
                } else if (message instanceof HeartBeatMessage) {
                    System.out.println("Slave received heartbeat");
                    masterMessenger.sendMessage(new HeartBeatMessage());
                }
                else {
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
