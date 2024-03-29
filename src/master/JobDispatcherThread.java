package master;

import jobs.Job;
import jobs.JobState;
import messages.JobMessage;
import messages.SocketMessenger;
import util.Host;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 7:13 PM
 * To change this template use File | Settings | File Templates.
 */

//invariant: this thread is the only one that ever removes jobs from the pq

//we keep a field in job with number of times a job has been tried
//if its been tried more than MAX_RETRIES times, NEVER WILL IT EVER BE DISPATCHED AGAIN
//THE JOB WILL DIE. FOREVER. AND EVER.

//when a slave dies, the scheduler should have a method called slaveDead()
//it will reschedule all jobs and make sure that slave doesnt get any more jobs
//it will also have a slaveAlive() again.

//we are not taking to account faulty slaves - a slave that is not dead, but that
//cant run jobs. SOLN: if too many jobs fail on slave, try restarting. if restart doesnt help either,
//maybe permanently delete the slave
public class JobDispatcherThread implements Runnable {

    private static int jobID = 0;
    private JobScheduler jobQueue;
    private ConcurrentHashMap<Host, SocketMessenger> messengers;
    private List<Job> runningJobs;

    public JobDispatcherThread(JobScheduler jobQueue, ConcurrentHashMap<Host, SocketMessenger> messengers,
                               List<Job> runningJobs) {
        this.jobQueue = jobQueue;
        this.messengers = messengers;
        this.runningJobs = runningJobs;
    }

    //should this busy-loop or have a timeout?
    public void run() {
        while (true) {
            if (!jobQueue.ready())
                continue;

            Job dispatchJob = jobQueue.dequeueNextJob();

            if (dispatchJob == null)
                continue;

            System.out.println("going to dispatch this job: " + dispatchJob);

            dispatchJob.state = JobState.DISPATCHED;
            dispatchJob.tries++;
            System.out.println("dispatching job: " + dispatchJob);
            SocketMessenger slaveToDispatchTo = messengers.get(dispatchJob.host);
            try {
                slaveToDispatchTo.sendMessage(new JobMessage(dispatchJob));
                runningJobs.add(dispatchJob);
            } catch (IOException e) {
                continue;
            }
            jobID++;
        }
    }
}
