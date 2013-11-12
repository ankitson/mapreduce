package master;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 7:16 PM
 * To change this template use File | Settings | File Templates.
 */

import jobs.Job;
import messages.JobMessage;
import messages.Message;
import messages.SocketMessenger;

import java.io.IOException;

/**
 * Listens for messages from a slave and services their requests
 */
public class SlaveListenerThread implements Runnable {

    private JobScheduler jobScheduler;
    private SocketMessenger slaveMessenger;
    private final int MAX_JOB_TRIES = 3; //read from config

    public SlaveListenerThread(SocketMessenger slaveMessenger, JobScheduler jobScheduler) {
        this.slaveMessenger = slaveMessenger;
        this.jobScheduler = jobScheduler;
    }

    public void run() {
        Message message;
        while (true) {
            try {
                message = slaveMessenger.receiveMessage();
                if (message instanceof JobMessage) {
                    Job job = ((JobMessage) message).job;
                    if (job.success == true) {
                        System.out.println(job + " completed successfully");
                        //add to user specific data structures here
                        //tell user where result of map/reduce is?
                    }
                    else { //job failed
                        if (job.tries == MAX_JOB_TRIES) {
                            System.out.println(job + " failed multiple times. Aborting");
                            //user specific data structures
                        } else {
                            System.out.println("Retrying " + job);
                            jobScheduler.addJob(job);
                        }
                    }
                } else {
                    System.err.println("Unexpected message received from slave: " + message);
                }
            } catch (IOException e) {
                System.err.println("Error receiving message from slave: " + e);
            } catch (ClassNotFoundException e) {
                System.err.println("Illegal message received from slave: " + e);
            }
        }
    }
}
