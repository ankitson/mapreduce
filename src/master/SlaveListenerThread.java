package master;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 7:16 PM
 * To change this template use File | Settings | File Templates.
 */

import dfs.DistributedFile;
import dfs.DistributedFileSystemConstants;
import jobs.Job;
import jobs.JobState;
import messages.*;
import util.FileUtils;
import util.Host;
import util.Pair;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Listens for messages from a slave and services their requests
 */
public class SlaveListenerThread implements Runnable {

    private JobScheduler jobScheduler;
    private SocketMessenger slaveMessenger;
    private ConcurrentHashMap<Host, SocketMessenger> messengers;
    private Map<File, DistributedFile> filesToDistributedFiles;
    private List<Job> chunkList;

    private ConcurrentMap<Integer, Pair<Integer, Stack<Job>>> mrJobSuccesses;

    private final int MAX_JOB_TRIES = 3; //read from config

    public SlaveListenerThread(SocketMessenger slaveMessenger, JobScheduler jobScheduler,
                               ConcurrentHashMap <Host, SocketMessenger> messengers,
                               Map<File, DistributedFile> filesToDistributedFiles, List<Job> chunkList,
                               ConcurrentMap<Integer, Pair<Integer, Stack<Job>>> mrJobSuccesses) {
        this.slaveMessenger = slaveMessenger;
        this.jobScheduler = jobScheduler;
        this.messengers = messengers;
        this.filesToDistributedFiles = filesToDistributedFiles;
        this.chunkList = chunkList;
        this.mrJobSuccesses = mrJobSuccesses;
    }

    public void run() {
        Message message;
        while (true) {
            try {
                message = slaveMessenger.receiveMessage();
                if (message instanceof JobMessage) {
                    JobMessage jm = ((JobMessage) message);
                    Job job = jm.job;
                    if (job.state == JobState.SUCCESS) {
                        jobScheduler.jobDone(job);
                        switch (job.jobType) {
                            case MAP:
                                successJob(job, null);

                                /*String mapOutFileName = jm.fileName;
                                HashSet<Host> hosts = new HashSet<Host>();
                                hosts.add(job.host);
                                Job nJob = new Job();
                                nJob.chunk = job.jobResultChunk;
                                nJob.reducerInterface = job.reducerInterface;
                                chunkList.add(nJob);*/

                                break;
                            case REDUCE:
                                System.out.println("master received reduce job message");

                                /*FileInfoMessage fim = ((FileInfoMessage) slaveMessenger.receiveMessage());
                                File reduceOutFile = new File(fim.getFileName());
                                slaveMessenger.receiveFile(reduceOutFile,(int) fim.getFileSize());
                                DistributedFile newDF = new DistributedFile(
                                        reduceOutFile, FileUtils.countLines(reduceOutFile)+1,messengers, DistributedFileSystemConstants.REPLICATION_FACTOR);
                                filesToDistributedFiles.put(reduceOutFile, newDF);
                                successJob(job, newDF);*/


                                /*List<Chunk> chunks = filesToDistributedFiles.get(reduceOutFile).getChunks();
                                for (Chunk chunk : chunks) {
                                    Job newJob = new Job();
                                    newJob.chunk = chunk;
                                    newJob.reducerInterface = job.reducerInterface;
                                    chunkList.add(newJob);
                                }*/
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
                } else if (message instanceof FileInfoMessage) {
                    System.out.println("master received FileInfoMessage");
                    FileInfoMessage fim = (FileInfoMessage) message;
                    File reduceOutFile = new File(fim.getFileName());
                    System.out.println("receivd fim: " + fim.getFileName());
                    slaveMessenger.receiveFile(reduceOutFile, (int) fim.getFileSize());
                    System.out.println("received file: " + reduceOutFile);
                    DistributedFile reduceDF = new
                            DistributedFile(reduceOutFile, FileUtils.countLines(reduceOutFile)+1,messengers,
                                DistributedFileSystemConstants.REPLICATION_FACTOR);
                } else if (message instanceof ReduceJobDoneMessage) {
                    ReduceJobDoneMessage rjdm = ((ReduceJobDoneMessage) message);
                    FileInfoMessage fim = (FileInfoMessage) rjdm.getFim();
                    Job completedJob = rjdm.getJob();
                    File reduceOutFile = new File(fim.getFileName());
                    slaveMessenger.receiveFile(reduceOutFile, (int) fim.getFileSize());
                    DistributedFile reduceDF = new
                            DistributedFile(reduceOutFile, FileUtils.countLines(reduceOutFile)+1,messengers,
                            DistributedFileSystemConstants.REPLICATION_FACTOR);
                    completedJob.jobResultChunk = reduceDF.getChunks().get(0);
                    filesToDistributedFiles.put(reduceOutFile, reduceDF);
                    jobScheduler.jobDone(completedJob);
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
                System.err.println("Error receiving message from slave (possibly timeout): " + e);
                return;
            } catch (ClassNotFoundException e) {
                System.err.println("Illegal message received from slave: " + e);
                return;
            }
        }
    }

    /*public void updateStatus(Job job) {
        List<Job> jobList = mrJobToJobs.get(job.mrJobID);
        for (int i=0;i<jobList.size();i++) {
            Job listJob = jobList.get(i);
            if (listJob.equals(job))
                jobList.set(i, job);
        }
        mrJobToJobs.put(job.mrJobID,jobList);
    }

    public boolean allSuccess(List<Job> jobs) {
        if (jobs == null)
            return false;

        boolean success = true;
        for (Job job : jobs) {
            if (job.state != JobState.SUCCESS)
                success = false;
        }
        return success;
    }*/

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

    private void successJob(Job job, DistributedFile df) {
        System.out.println(job + " completed successfully");

        int mrJobID = job.mrJobID;
        Pair<Integer, Stack<Job>> current = mrJobSuccesses.get(mrJobID);
        if (current.getSecond().size() == current.getFirst() - 1) {
            System.out.println("Map Reduce Job Successfully Completed: " + mrJobID);
            System.out.println("Result file is at: " + df);
        }
        else
            current.getSecond().push(job);
    }
}
