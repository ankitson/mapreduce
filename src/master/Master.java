package master;

import dfs.Chunk;
import dfs.DistributedFile;
import jobs.Job;
import jobs.MapReduceJob;
import messages.SocketMessenger;
import util.Host;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

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

    //number of slaves
    //master will wait until all slaves connect
    int NUMBER_OF_SLAVES = 3;

    public Set<Host> slaves; //parse from config file //MAKE PRIVATE LATER
    public Set<File> files; //parse from config file //MAKE PRIVATE LATER
    private Map<File, DistributedFile> filesToDistributedFiles;
    private ConcurrentHashMap<Host, SocketMessenger> messengers;

    private ConcurrentMap<Integer, List<Job>> mrJobToInternalJobs;

    private ConcurrentHashMap<Integer, MapReduceJob> mrIDtoJob;

    //private List<Chunk> mapOutputChunks;

    private JobScheduler jobQueue;

    List<Job> runningJobs;

    private int mapReduceJobID = 0;
    private AtomicInteger internalJobID = new AtomicInteger(0);


    //yolo constructor for testing fix later
    public Master(Set<File> files, Set<Host> slaves) throws IOException {
        this.files = files;
        this.slaves = slaves;

        messengers = new ConcurrentHashMap<Host,SocketMessenger>();
        new Thread(new SlaveJoinThread(messengers)).start();
        System.out.println("Waiting for all slaves to connect.");

        //if some slave never starts, it will infinite loop
        while (messengers.size() < NUMBER_OF_SLAVES) {
            ;
        }

        runningJobs = Collections.synchronizedList(new ArrayList<Job>());

        mrJobToInternalJobs = new ConcurrentHashMap<Integer, List<Job>>();
        //mapOutputChunks = Collections.synchronizedList(new ArrayList<Chunk>());

        //DFS assumes that slaves dont die
        System.out.println("messengers after init: " + messengers);
        filesToDistributedFiles = new HashMap<File, DistributedFile>();
        initializeDFS(files);
        System.out.println("messengers after dfs: " + messengers);


        new Thread(new HealthCheckerThread(messengers)).start();

        Chunk wordCountChunk = filesToDistributedFiles.get(new File("./wordcounttest.txt")).getChunks().get(0);
        Chunk floatChunk1 = filesToDistributedFiles.get(new File("./floatyolotest.txt")).getChunks().get(0);

        Chunk reducewc1 = filesToDistributedFiles.get(new File("./reducewc1.txt")).getChunks().get(0);
        Chunk reducewc2 = filesToDistributedFiles.get(new File("./reducewc2.txt")).getChunks().get(0);



        //chunk arg ONLY FOR TESTING
        jobQueue = new JobScheduler(reducewc1,reducewc2);
        System.out.println("messengers after jobschdule: " + messengers);




        new Thread(new JobDispatcherThread(jobQueue, messengers, runningJobs)).start(); //fill in args to thread
        for (SocketMessenger slaveMessenger : messengers.values()) {
            new Thread(new SlaveListenerThread(slaveMessenger, jobQueue,
                    messengers, runningJobs, mrJobToInternalJobs, filesToDistributedFiles, internalJobID,
                    mrIDtoJob)).start();
            //fill in other args to thread ?
        }
    }

    private void initializeDFS(Set<File> files) throws IOException {
        for (File file : files) {
            filesToDistributedFiles.put(file, new DistributedFile(file, messengers));
        }
    }

    public void listenInput() {
        System.out.println("Welcome to MapReduce!");
        System.out.println("Connected slaves:" + slaves);

        Scanner in = new Scanner(System.in);
        while (true) {
            String line = in.nextLine();
            if (line.equals("jobs")) {
                for (Job job : runningJobs) {
                    System.out.println(job);
                }
            } else if (line.equals("files")) {
                for (DistributedFile df : filesToDistributedFiles.values()) {
                    System.out.println(df);
                }
            } else if (line.equals("slaves")) {
                for (Host host : messengers.keySet()) {
                    System.out.println(host.HOSTNAME);
                }
            } else if (line.startsWith("run")) {
                String[] args = line.split(" ");
                if (args.length < 2) {
                    System.out.println("invalid input");
                }
                String className = args[1];
                try {
                    MapReduceJob mrj = (MapReduceJob) Class.forName(className).newInstance();
                    boolean status = generateJobs(mrj);
                    if (status == true)
                        mapReduceJobID++;

                } catch (ClassNotFoundException e) {
                    System.out.println("Unable to find mapreduce job class: " + e);
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (ClassCastException e) {
                    e.printStackTrace();
                }
            }

        }

        //every time the user inputs an add job,
        //job.jobID = jobID
        //jobID++
        //jobQueue.add(job)


    }

    public boolean generateJobs(MapReduceJob mrj) {
        System.out.println("fdf: " + filesToDistributedFiles);
        File inputFile = new File(mrj.getInputFileName());
        System.out.println("inputFIle: " + inputFile);
        System.out.println("fdd conatins: " + filesToDistributedFiles.containsKey(inputFile));
        List<Chunk> jobChunks = filesToDistributedFiles.get(new File(mrj.getInputFileName())).getChunks();
        List<Job> mapJobs = new ArrayList<Job>();
        for (Chunk chunk : jobChunks) {
            Job mapJob = new Job(mapReduceJobID, internalJobID.get(), null, mrj.getMapper(), chunk, null);
            mapJobs.add(mapJob);
            internalJobID.incrementAndGet();
        }
        System.out.println("generated new mapreduce job subjobs");
        System.out.println(mrJobToInternalJobs);
        mrJobToInternalJobs.put(mapReduceJobID, mapJobs);
        boolean status = jobQueue.addJobs(mapJobs);
        if (status == true)
            mrIDtoJob.put(mapReduceJobID, mrj);

        return status;
    }

    public static void main(String[] args) throws IOException {
        Set<File> files = new HashSet<File>();
        Set<Host> slaves = new HashSet<Host>();
        files.add(new File("./testfile1.txt"));
        files.add(new File("./testfile2.txt"));
        files.add(new File("./testfile3.txt"));
        files.add(new File("./testfile4.txt"));
        files.add(new File("./testfile5.txt"));
        files.add(new File("./wordcounttest.txt"));
        files.add(new File("./floatyolotest.txt"));
        files.add(new File("./reducewc1.txt"));
        files.add(new File("./reducewc2.txt"));
        files.add(new File("wordcount.txt"));
        System.out.println("files to chunk: " + files);
        slaves.add(new Host("UNIX2.ANDREW.CMU.EDU", 6666));
        slaves.add(new Host("UNIX3.ANDREW.CMU.EDU", 6666));
        slaves.add(new Host("UNIX4.ANDREW.CMU.EDU", 6666));
        Master master = new Master(files, slaves);
        master.listenInput();
    }

}