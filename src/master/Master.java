package master;

import dfs.DistributedFile;
import messages.SocketMessenger;
import util.Host;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 6:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class Master {

    //private final Configuration MAPREDUCE_CONFIG = new Configuration(new File('./config/mapreduce_config.txt'));
    //private final Configuration DFS_CONFIG = new Configuration(new File('./config/dfs_config.txt'));

    public Set<Host> slaves; //parse from config file //MAKE PRIVATE LATER
    public Set<File> files; //parse from config file //MAKE PRIVATE LATER
    private Map<File, DistributedFile> filesToDistributedFiles;
    private ConcurrentHashMap<Host, SocketMessenger> messengers;

    private JobScheduler jobQueue;

    private int jobID = 0;


    //yolo constructor for testing fix later
    public Master(Set<File> files, Set<Host> slaves) throws IOException {
        this.files = files;
        this.slaves = slaves;
        initializeNodeMessengers();
        System.out.println("messengers after init: " + messengers);
        filesToDistributedFiles = new HashMap<File, DistributedFile>();
        initializeDFS(files);
        System.out.println("messengers after dfs: " + messengers);

        jobQueue = new JobScheduler();
        System.out.println("messengers after jobschdule: " + messengers);

        new Thread(new JobDispatcherThread(jobQueue, messengers)).start(); //fill in args to thread
        for (SocketMessenger slaveMessenger : messengers.values()) {
            new Thread(new SlaveListenerThread(slaveMessenger, jobQueue, messengers)).start();
            //fill in other args to thread ?
        }
    }

    private void initializeDFS(Set<File> files) throws IOException {
        for (File file : files) {
            filesToDistributedFiles.put(file, new DistributedFile(file, messengers));
        }
    }

    private void initializeNodeMessengers() {
        messengers = new ConcurrentHashMap<Host,SocketMessenger>();
        for (Host slave : slaves) {
            try {
                messengers.put(slave, new SocketMessenger(slave.getSocket()));
            } catch (IOException e) {
                System.err.println("Unable to connect to slave " + slave + ". Message:  " + e);
            }
        }
        System.out.println("Messengers initialized to: " + messengers);
    }

    public void listenInput() {
        System.out.println("Welcome to MapReduce!");
        System.out.println("Connected slaves:" + slaves);

        //every time the user inputs an add job,
        //job.jobID = jobID
        //jobID++
        //jobQueue.add(job)


    }

    public static void main(String[] args) throws IOException {
        Set<File> files = new HashSet<File>();
        Set<Host> slaves = new HashSet<Host>();
        files.add(new File("./testfile1.txt"));
        files.add(new File("./testfile2.txt"));
        files.add(new File("./testfile3.txt"));
        files.add(new File("./testfile4.txt"));
        System.out.println("files to chunk: " + files);
        slaves.add(new Host("unix1.andrew.cmu.edu", 6666));
        Master master = new Master(files, slaves);
        master.listenInput();

    }
}