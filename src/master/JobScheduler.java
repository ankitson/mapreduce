package master;

import dfs.Chunk;
import example.WordCountCombiner;
import example.WordCountMapper;
import jobs.Job;
import jobs.JobType;
import util.Host;
import util.Pair;

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

    //CHUNK ARG ONLY FOR TESTING
    public JobScheduler(Chunk chunk1, Chunk chunk2) {
        jobQueue = new PriorityBlockingQueue<Job>(1, new JobComparator());

        //ONLY FOR TESTING
        //Job dummyJob = new Job(0, new Host("unix1.andrew.cmu.edu", 6666), JobType.DUMMY);

        /*Job mapJob = new Job(1, new Host("UNIX2.ANDREW.CMU.EDU", 6666), JobType.MAP);

        mapJob.chunk = chunk;
        mapJob.mapperInterface = new FloatMapper(new FloatCombiner());
        mapJob.recordRange = new Pair<Integer,Integer>(1,20);

        //jobQueue.add(dummyJob);
        jobQueue.add(mapJob);*/

        //ReducerInterface reducer = new WordCountReducer();
        //Job reduceJob = new Job(1, new Host("UNIX2.ANDREW.CMU.EDU", 6666), reducer, chunk1, chunk2);
        //jobQueue.add(reduceJob);

        Job mapJob = new Job(1, new Host("UNIX2.ANDREW.CMU.EDU", 6666), JobType.MAP);
        mapJob.chunk = chunk1;
        mapJob.mapperInterface = new WordCountMapper(new WordCountCombiner());
        mapJob.recordRange = new Pair<Integer,Integer>(1,20);
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
