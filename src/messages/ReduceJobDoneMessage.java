package messages;

import jobs.Job;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/20/13
 * Time: 2:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class ReduceJobDoneMessage extends Message {
    Job job;
    FileInfoMessage fim;

    public ReduceJobDoneMessage(Job job, FileInfoMessage fim) {
        this.job = job;
        this.fim = fim;
    }

    public FileInfoMessage getFim() {
        return fim;
    }

    public Job getJob() {
        return job;
    }

}
