package master;

import jobs.Job;
import jobs.JobType;
import util.Host;

import java.util.Comparator;
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
    private boolean ready;

    public JobScheduler() {
        jobQueue = new PriorityBlockingQueue<Job>(1, new JobComparator());

        //ONLY FOR TESTING
        //Job dummyJob = new Job(0, new Host("unix1.andrew.cmu.edu", 6666), JobType.DUMMY);
        Job mapJob = new Job(1, new Host("UNIX2.ANDREW.CMU.EDU", 6666), JobType.MAP);
        //jobQueue.add(dummyJob);
        jobQueue.add(mapJob);
        ready = true;
    }

    //if a job has already been tried, maybe try to schedule it on a different node?
    public boolean addJob(Job job) {
        //set the host for this job - decide which slave to run this job on
        //job.host = x
        return jobQueue.add(job);
    }

    public Job dequeueNextJob() {
        //ONLY FOR TESTING
        ready = false;

        return jobQueue.poll();



    }

    //change addJob to addJobs(list), ready after whole batch added
    public boolean ready() {
        return ready;
    }

    public void slaveDied(Host slave) {
        return; //reschedule
    }

    public void slaveAlive(Host slave) {
        return; //reschedule
    }

    class JobComparator implements Comparator<Job> {
        public int compare(Job j1, Job j2) {
            return 1;
        }
    }

}
