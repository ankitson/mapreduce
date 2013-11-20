package master;

import dfs.Chunk;
import jobs.Job;
import jobs.JobState;
import jobs.JobType;
import jobs.ReducerInterface;
import messages.SocketMessenger;
import util.Host;
import util.KCyclicIterator;

import java.util.*;
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

    private Map<Integer, List<Job>> mrJobToMapJobs;
    private Map<Integer, List<Job>> mrJobToReduceJobs;
    private Map<Integer, List<Job>> completedReduceJobs;

    //CHUNK ARG ONLY FOR TESTING
    public JobScheduler(Chunk chunk1, List<Job> chunkList, AtomicInteger internalJobID,
                        ConcurrentMap<Host,SocketMessenger> messengers) {

        jobQueue = new PriorityBlockingQueue<Job>(1, new JobComparator());

        this.chunkList = chunkList;
        this.internalJobID = internalJobID;
        this.messengers = messengers;
        this.mrJobToMapJobs = new HashMap<Integer, List<Job>>();
        this.mrJobToReduceJobs = new HashMap<Integer, List<Job>>();
        this.completedReduceJobs = new HashMap<Integer, List<Job>>();

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

        System.out.println("job queue after addJobs: " + jobQueue);
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
        /*if (chunkList.size() >= 2) {
            Job reduceJob;
            Job job1 = chunkList.remove(0);
            Job job2 = chunkList.remove(0);
            Chunk chunk1 = job1.chunk;
            Chunk chunk2 = job2.chunk;
            ReducerInterface reducer = job1.reducerInterface;
            Host host = chunk1.getHosts().iterator().next();
            reduceJob = new Job(job1.mrJobID, internalJobID.incrementAndGet(), host, reducer, chunk1, chunk2, JobState.QUEUED);
            return reduceJob;
        }*/

        Job next = jobQueue.poll();
        if (next != null) {
            System.out.println("dequeue next job called");

            System.out.println("returning: " + next);
        }
        return next;
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

    public void jobDone(Job job) {
        ready = false;
        int mrJobID = job.mrJobID;
        System.out.println("job done in scheduler: " + job);


        if (job.jobType == JobType.REDUCE) {
            System.out.println("reduce job done");
            List<Job> completedReduces = completedReduceJobs.get(mrJobID);
            System.out.println("current compl. reduces: " + completedReduces);
            if (completedReduces == null) {
                completedReduces = new ArrayList<Job>();
            }
            completedReduces.add(job);
            completedReduceJobs.put(mrJobID, completedReduces);
            System.out.println("completed reduces after new add: " + completedReduces);

            List<Job> newReduceJobs = new ArrayList<Job>();
            while (completedReduces.size() >= 2) {
                Job reduce1 = completedReduces.remove(0);
                Job reduce2 = completedReduces.remove(0);
                ReducerInterface reducer = reduce1.reducerInterface;
                Chunk chunk1 = reduce1.jobResultChunk;
                Chunk chunk2 = reduce2.jobResultChunk;
                Job newReduce = new Job(mrJobID, internalJobID.incrementAndGet(),null, reducer, chunk1, chunk2, null);
                newReduceJobs.add(newReduce);
            }
            if (newReduceJobs.size() > 0) {
                System.out.println("adding new reduce jobs to queue: " + newReduceJobs);
                addJobs(newReduceJobs);
            }
            //spawn new reduce jobs out of existing reduce jobs
        }


        else if (job.jobType == JobType.MAP) {
            List<Job> mrJobMapJobs = mrJobToMapJobs.get(mrJobID);

            //update job status
            for (int i=0; i<mrJobMapJobs.size();i++) {
                Job job2 = mrJobMapJobs.get(i);
                if (job2.equals(job)) {
                    mrJobMapJobs.set(i, job);
                }
            }

            System.out.println("map jobs: " + mrJobMapJobs);
            //check if map phase of MR job is done
            boolean mapCompleted = true;
            for (int i=0;i<mrJobMapJobs.size();i++) {
                Job jobI = mrJobMapJobs.get(i);
                if (jobI.state != JobState.SUCCESS)
                    mapCompleted = false;
            }

            //create reduce jobs
            if (mapCompleted) {
                initializeReduceJobs(mrJobID);
            }
        }
        ready = true;
    }

    public void initializeMapJobs(int mrJobID, List<Job> mapJobs) {
        mrJobToMapJobs.put(mrJobID, mapJobs);
        addJobs(mapJobs);
    }

    public void initializeReduceJobs(int mrJobID) {
        List<Job> reduceJobs = new ArrayList<Job>();
        List<Job> mapJobs = mrJobToMapJobs.get(mrJobID);
        for (int i=0;i<mapJobs.size()/2;i++) {
            Chunk chunk1 = mapJobs.get(2*i).jobResultChunk;
            Chunk chunk2 = mapJobs.get(2*i+1).jobResultChunk;
            ReducerInterface reducer = mapJobs.get(i).reducerInterface;
            Job reduceJob = new Job(mrJobID, internalJobID.incrementAndGet(), null, reducer, chunk1, chunk2, null);
            reduceJobs.add(reduceJob);
        }
        System.out.println("Reduce jobs initialized to: " + reduceJobs);
        mrJobToReduceJobs.put(mrJobID, reduceJobs);
        addJobs(reduceJobs);

        /*if (mapJobs.size() % 2 == 1) {
            List<Job> completedReduces = completedReduceJobs.get(mrJobID);
            completedReduces.add(mapJobs.get(mapJobs.size() - 1));
        }*/



        System.out.println("after add, queue: " + jobQueue);
    }

}
