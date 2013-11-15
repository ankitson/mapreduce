package jobs;

import dfs.Chunk;
import util.Host;
import util.Pair;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 11:05 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * Represents any type of job - map/reduce/combine/dummy
 *
 * Set the fields you don't need to be null
 *
 * We need unused K for reducer jobs too
 */
public class Job<K extends Serializable & Comparable<K>,V extends Serializable> implements Serializable {

    //for general jobs
    public int mrJobID;
    public int internalJobID;
    public Host host; //the host the job is running on
    public int tries; //number of times job has been tried before
    public JobState state;
    public JobType jobType;
    public Chunk jobResultChunk;

    //map jobs
    public Chunk chunk;
    public MapperInterface mapperInterface; //TESTING
    //public MapperInterface mapperInterface;
    public Pair<Integer, Integer> recordRange;

    //reduce jobs
    public ReducerInterface reducerInterface;
    public Chunk chunk1;
    public Chunk chunk2;

    public Job() {

    }
    public Job(int mrJobID, int jobID, Host host, JobType jobType, JobState jobState) {
        this.mrJobID = mrJobID;
        this.internalJobID = jobID;
        this.host = host;
        this.jobType = jobType;
        this.state = jobState;
        this.jobResultChunk = null;
        tries = 0;
        recordRange = null;

        chunk = null;
        mapperInterface = null;
        reducerInterface = null;
        chunk1 = null;
        chunk2 = null;
    }

    //shortcut for common case
    public Job(int mrJobID, int jobID, Host host, int tries, boolean success, JobType jobType, JobState jobState) {
        this(mrJobID, jobID, host, jobType, jobState);
        this.state = jobState;
        this.tries = 0;
    }

    //shortcut for map jobs
    //public Job(int jobID, Host host, MapperInterface<K,V> mapperInterface, Chunk chunk) { TESTING
    public Job(int mrJobID, int jobID, Host host, MapperInterface mapperInterface, Chunk chunk, JobState jobState) {
        this(mrJobID, jobID, host, JobType.MAP, jobState);
        this.mapperInterface = mapperInterface;
        this.chunk = chunk;
        this.recordRange = chunk.getRecordRange();
    }

    //shortcut for reduce jobs
    public Job(int mrJobID, int jobID, Host host, ReducerInterface reducerInterface, Chunk chunk1, Chunk chunk2, JobState jobState) {
        this(mrJobID, jobID, host, JobType.REDUCE, jobState);
        this.reducerInterface = reducerInterface;
        this.chunk1 = chunk1;
        this.chunk2 = chunk2;
    }

    public String toString() {
        return String.format("[%d]: [%s] job on [%s] (%s) (result: %s)",internalJobID, jobType, host, state, jobResultChunk);
    }

    public boolean equals(Object other) {
        if (!(other instanceof Job))
            return false;

        return internalJobID == ((Job) other).internalJobID;
    }
}
