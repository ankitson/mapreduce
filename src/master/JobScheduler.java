package master;

import dfs.Chunk;
import jobs.Job;
import jobs.JobState;
import jobs.ReducerInterface;
import messages.SocketMessenger;
import util.Host;
import util.KCyclicIterator;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
    private List<Job> chunkList;
    private boolean ready;
    private AtomicInteger internalJobID;
    private ConcurrentMap<Host, SocketMessenger> messengers;

    //CHUNK ARG ONLY FOR TESTING
    public JobScheduler(Chunk chunk1, List<Job> chunkList, AtomicInteger internalJobID,
                        ConcurrentMap<Host,SocketMessenger> messengers) {

        jobQueue = new PriorityBlockingQueue<Job>(1, new JobComparator());

        this.chunkList = chunkList;
        this.internalJobID = internalJobID;
        this.messengers = messengers;

        ready = true;
    }

    //if a job has already been tried, maybe try to schedule it on a different node?
    public boolean addJob(Job job) {
        //set the host for this job - decide which slave to run this job on
        //job.host = x

        return jobQueue.add(job);
    }

    public boolean addJobs(List<Job> jobs) {
        ready = false;
        //FILL THIS IN

        KCyclicIterator<Map.Entry<Host,SocketMessenger>> messengerIterator =
                new KCyclicIterator<Map.Entry<Host, SocketMessenger> >(messengers.entrySet(), 1);
        for (Job job : jobs) {
            Map.Entry<Host,SocketMessenger> hostMessenger = messengerIterator.next().get(0);
            Host host = hostMessenger.getKey();
            SocketMessenger messenger = hostMessenger.getValue();
            job.host = host;
            job.state = JobState.QUEUED;
            jobQueue.add(job);

        }
        /*List<Host> hosts = new ArrayList<Host>();
        hosts.add(new Host("UNIX2.ANDREW.CMU.EDU", 6666));
        hosts.add(new Host("UNIX3.ANDREW.CMU.EDU", 6666));
        hosts.add(new Host("UNIX4.ANDREW.CMU.EDU", 6666));
        int i = 0;
        for (Job job : jobs) {
            job.host = hosts.get(i);
            job.state = JobState.QUEUED;
            i++;
            if (i == 3)
                i = 0;
            jobQueue.add(job);
        }*/
        ready = true;
        return true;
    }

    public Job dequeueNextJob() {
        if (chunkList.size() >= 2) {
            Job reduceJob;
            Job job1 = chunkList.remove(0);
            Job job2 = chunkList.remove(0);
            Chunk chunk1 = job1.chunk;
            Chunk chunk2 = job2.chunk;
            ReducerInterface reducer = job1.reducerInterface;
            Host host = chunk1.getHosts().iterator().next();
            reduceJob = new Job(job1.mrJobID, internalJobID.incrementAndGet(), host, reducer, chunk1, chunk2, JobState.QUEUED);
            return reduceJob;
        }

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
