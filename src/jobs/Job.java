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
 */
public class Job<V> implements Serializable {

    //for general jobs
    public int jobID;
    public Host host; //the host the job is running on
    public int tries; //number of times job has been tried before
    public boolean success; //whether job successful or not
    public JobType jobType;

    //map jobs
    public Chunk chunk;
    public MapperInterface<V> mapperInterface;

    //reduce jobs
    public ReducerInterface<V> reducerInterface;
    public Pair<Host, Host> reduceHosts;

    public Job(int jobID, Host host, JobType jobType) {
        this.jobID = jobID;
        this.host = host;
        this.jobType = jobType;
        tries = 0;
        success = false;

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
    public Job(int jobID, Host host, MapperInterface<V> mapperInterface, Chunk chunk) {
        this(jobID, host, JobType.MAP);
        this.mapperInterface = mapperInterface;
        this.chunk = chunk;
    }

    //shortcut for reduce jobs
    public Job(int jobID, Host host, ReducerInterface<V> reducerInterface, Pair<Host, Host> reduceHosts) {
        this(jobID, host, JobType.REDUCE);
        this.reducerInterface = reducerInterface;
        this.reduceHosts = reduceHosts;
    }

    public String toString() {
        return String.format("[%d]: [%s] job on [%s] (%s)",jobID, jobType, host, success);
    }
}
