package messages;

import jobs.Job;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 7:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class JobMessage extends Message {

    public Job job;
    public JobMessage(Job job) {
        this.job = job;
    }

    public String toString() {
        return job.toString();
    }
}
