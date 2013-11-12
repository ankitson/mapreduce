package messages;

import jobs.Job;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 12:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class StatusResponseMessage extends Message {

    List<Job> runningJobs;

    public StatusResponseMessage(List<Job> runningJobs) {
        this.runningJobs = runningJobs;
    }
}
