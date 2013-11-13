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
public class Job<K extends Serializable,V extends Serializable> implements Serializable {

    //for general jobs
    public int internalJobID;
    public Host host; //the host the job is running on
    public int tries; //number of times job has been tried before
    public boolean success; //whether job successful or not
    public JobType jobType;

    //map jobs
    public Chunk chunk;
    //public MapperInterface<K,V> mapperInterface; TESTING
    public MapperInterface mapperInterface;
    public Pair<Integer, Integer> recordRange;

    //reduce jobs
    public ReducerInterface<V> reducerInterface;
    public Pair<Host, Host> reduceHosts;

    public Job(int jobID, Host host, JobType jobType) {
        this.internalJobID = jobID;
        this.host = host;
        this.jobType = jobType;
        tries = 0;
        success = false;
        recordRange = null;

        chunk = null;
        mapperInterface = null;
        reducerInterface = null;
        reduceHosts = null;
    }

    //shortcut for common case
    public Job(int jobID, Host host, int tries, boolean success, JobType jobType) {
        this(jobID, host, jobType);
        this.success = success;
        this.tries = 0;
    }

    //shortcut for map jobs
    //public Job(int jobID, Host host, MapperInterface<K,V> mapperInterface, Chunk chunk) { TESTING
    public Job(int jobID, Host host, MapperInterface mapperInterface, Chunk chunk) {
        this(jobID, host, JobType.MAP);
        this.mapperInterface = mapperInterface;
        this.chunk = chunk;
        this.recordRange = chunk.getRecordRange();
    }

    //shortcut for reduce jobs
    public Job(int jobID, Host host, ReducerInterface<V> reducerInterface, Pair<Host, Host> reduceHosts) {
        this(jobID, host, JobType.REDUCE);
        this.reducerInterface = reducerInterface;
        this.reduceHosts = reduceHosts;
    }

    public String toString() {
        return String.format("[%d]: [%s] job on [%s] (%s)",internalJobID, jobType, host, success);
    }

    public boolean equals(Object other) {
        if (!(other instanceof Job))
            return false;

        return internalJobID == ((Job) other).internalJobID;
    }
}
