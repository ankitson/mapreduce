package master;

import jobs.Job;
import util.Host;

import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 8:20 PM
 * To change this template use File | Settings | File Templates.
 */

//fill in the logic for rescheduling,
//and custom comparator for jobs

//jobs are only added in 2 place
public class JobScheduler {

    private PriorityBlockingQueue<Job> jobQueue;

    public JobScheduler() {
        jobQueue = new PriorityBlockingQueue<Job>();
    }

    public boolean addJob(Job job) {
        //set the host for this job - decide which slave to run this job on
        //job.host = x
        return jobQueue.add(job);
    }

    public Job dequeueNextJob() {
        return jobQueue.poll();
    }

    //return null if there is no next job
    public Job viewNextJob() {
        return null;
    }

    public void slaveDied(Host slave) {
        return; //reschedule
    }

    public void slaveAlive(Host slave) {
        return; //reschedule
    }

}
