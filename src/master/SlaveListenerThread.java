package master;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 7:16 PM
 * To change this template use File | Settings | File Templates.
 */

import dfs.Chunk;
import dfs.DistributedFile;
import dfs.DistributedFileSystemConstants;
import jobs.Job;
import jobs.JobState;
import messages.*;
import util.FileUtils;
import util.Host;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Listens for messages from a slave and services their requests
 */
public class SlaveListenerThread implements Runnable {

    private JobScheduler jobScheduler;
    private SocketMessenger slaveMessenger;
    private ConcurrentHashMap<Host, SocketMessenger> messengers;
    private Map<File, DistributedFile> filesToDistributedFiles;
    private List<Job> completedMaps;
    private List<Job> chunkList;

    private final int MAX_JOB_TRIES = 3; //read from config

    public SlaveListenerThread(SocketMessenger slaveMessenger, JobScheduler jobScheduler,
                               ConcurrentHashMap <Host, SocketMessenger> messengers,
                               Map<File, DistributedFile> filesToDistributedFiles, List<Job> completedMaps, List<Job> chunkList) {
        this.slaveMessenger = slaveMessenger;
        this.jobScheduler = jobScheduler;
        this.messengers = messengers;
        this.filesToDistributedFiles = filesToDistributedFiles;
        this.completedMaps = completedMaps;
        this.chunkList = chunkList;
    }

    public void run() {
        Message message;
        while (true) {
            try {
                message = slaveMessenger.receiveMessage();
                if (message instanceof JobMessage) {
                    JobMessage jm = ((JobMessage) message);
                    Job job = jm.job;
                    //updateJobStatus(job);
                    if (job.state == JobState.SUCCESS) {
                        System.out.println(job + " completed successfully");
                        switch (job.jobType) {
                            case MAP:
                                String mapOutFileName = jm.fileName;
                                HashSet<Host> hosts = new HashSet<Host>();
                                hosts.add(job.host);
                                completedMaps.add(job);
                                Job nJob = new Job();
                                nJob.chunk = job.jobResultChunk;
                                nJob.reducerInterface = job.reducerInterface;
                                chunkList.add(nJob);
                                break;
                            case REDUCE:
                                FileInfoMessage fim = ((FileInfoMessage) slaveMessenger.receiveMessage());
                                File reduceOutFile = new File(fim.getFileName());
                                slaveMessenger.receiveFile(reduceOutFile,(int) fim.getFileSize());
                                DistributedFile newDF = new DistributedFile(
                                        reduceOutFile, FileUtils.countLines(reduceOutFile)+1,messengers, DistributedFileSystemConstants.REPLICATION_FACTOR);
                                filesToDistributedFiles.put(reduceOutFile, newDF);


                                List<Chunk> chunks = filesToDistributedFiles.get(reduceOutFile).getChunks();
                                completedMaps.add(job);
                                for (Chunk chunk : chunks) {
                                    Job newJob = new Job();
                                    newJob.chunk = chunk;
                                    newJob.reducerInterface = job.reducerInterface;
                                    chunkList.add(newJob);
                                }
                                //MAKE NEW REDUCE JOB ON THIS REDUCED CHUNK  + OTHER CHUNKS
                                break;
                            case DUMMY:
                                break;
                        }

                        //add to user specific data structures here
                        //tell user where result of map/reduce is?
                        //runningJobs.remove(job);
                    }
                    else { //job failed
                        if (job.tries == MAX_JOB_TRIES) {
                            System.out.println(job + " failed multiple times. Aborting");
                        } else {
                            System.out.println("Retrying " + job);
                            jobScheduler.addJob(job);
                        }
                    }
                } else if (message instanceof HeartBeatMessage) {
                    //System.out.println("Slave heartbeat OK");
                }
            } catch (SocketTimeoutException e) {
                System.err.println("Slave died!");

                //remove the messenger for this slave
                Iterator<Map.Entry<Host, SocketMessenger>> entryIterator = messengers.entrySet().iterator();
                while (entryIterator.hasNext()) {
                    Map.Entry<Host, SocketMessenger> entry = entryIterator.next();
                    Host host = entry.getKey();
                    SocketMessenger messenger = entry.getValue();
                    InetAddress inetAddress = messenger.socket.getInetAddress();
                    InetAddress slaveMessengerAddress = slaveMessenger.socket.getInetAddress();
                    if (inetAddress.equals(slaveMessengerAddress)) {
                        entryIterator.remove();
                        jobScheduler.slaveDied(host);
                    }
                }
                return; //kill this thread
            } catch (IOException e) {
                // we dont terminate the slave connection here - if it is a one off bad message , it will be ignored
                // if the socket has been closed because the slave is dead, the thread will eventually die due to
                // a timeout on the heartbeat
                System.err.println("Error receiving message from slave (possibly timeout): " + e);
                return;
            } catch (ClassNotFoundException e) {
                System.err.println("Illegal message received from slave: " + e);
                return;
            }
        }
    }

    /*public void addSuccessfulMapJob(Job job) {
        if (job.state != JobState.SUCCESS || job.jobType != JobType.MAP)
            return;

        int mrJobID = job.mrJobID;

        List<Job> currentSuccess = successMapJobs.get(mrJobID);
        if (currentSuccess == null) {
            List<Job> newList = new ArrayList<Job>();
            newList.add(job);
            successMapJobs.put(mrJobID, newList);
        } else {
            currentSuccess.add(job);
            successMapJobs.put(mrJobID, currentSuccess);
        }

    }*/
}
