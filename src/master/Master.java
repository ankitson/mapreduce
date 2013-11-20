package master;

import dfs.Chunk;
import dfs.DistributedFile;
import dfs.DistributedFileSystemConstants;
import jobs.Job;
import jobs.MapReduceJob;
import messages.SocketMessenger;
import util.Host;
import util.Pair;

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

    //number of slaves
    //master will wait until all slaves connect
    int NUMBER_OF_SLAVES = 2;

    public Set<Host> slaves; //parse from config file //MAKE PRIVATE LATER
    public Set<File> files; //parse from config file //MAKE PRIVATE LATER
    private ConcurrentMap<File, DistributedFile> filesToDistributedFiles;
    private ConcurrentHashMap<Host, SocketMessenger> messengers;
    private List<Job> chunkList;

    private ConcurrentMap<Integer, Pair<Integer, Stack<Job>>> mrJobSuccesses;


    private JobScheduler jobQueue;

    List<Job> runningJobs;

    private int mapReduceJobID = 0;
    private AtomicInteger internalJobID  = new AtomicInteger(0);


    //yolo constructor for testing fix later
    public Master(Set<File> files, Set<Host> slaves) throws IOException {
        this.files = files;
        this.slaves = slaves;
        runningJobs = Collections.synchronizedList(new ArrayList<Job>());
        messengers = new ConcurrentHashMap<Host,SocketMessenger>();
        chunkList = Collections.synchronizedList(new ArrayList<Job>());
        mrJobSuccesses = new ConcurrentHashMap<Integer, Pair<Integer, Stack<Job>>>();
        filesToDistributedFiles = new ConcurrentHashMap<File, DistributedFile>();

        new Thread(new SlaveJoinThread(messengers)).start();
        System.out.println("Waiting for all slaves to connect.");

        //if some slave never starts, it will infinite loop
        while (messengers.size() < NUMBER_OF_SLAVES) {
            ;
        }

        initializeDFS(files);

        jobQueue = new JobScheduler(filesToDistributedFiles.get(new File("wordcount.txt")).getChunks().get(0),
                chunkList, internalJobID, messengers);

        new Thread(new HealthCheckerThread(messengers)).start();
        new Thread(new JobDispatcherThread(jobQueue, messengers, runningJobs)).start();
        for (SocketMessenger slaveMessenger : messengers.values()) {
            new Thread(new SlaveListenerThread(slaveMessenger, jobQueue, messengers,
                    filesToDistributedFiles, chunkList, mrJobSuccesses)).start();
        }
    }

    private void initializeDFS(Set<File> files) throws IOException {
        for (File file : files) {
            filesToDistributedFiles.put(file, new DistributedFile(file, DistributedFileSystemConstants.SPLIT_SIZE,messengers,DistributedFileSystemConstants.REPLICATION_FACTOR));
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
                    generateJobs(mrj);
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
    }

    public void generateJobs(MapReduceJob mrj) {
        File inputFile = new File(mrj.getInputFileName());
        List<Chunk> jobChunks = filesToDistributedFiles.get(new File(mrj.getInputFileName())).getChunks();
        List<Job> mapJobs = new ArrayList<Job>();
        for (Chunk chunk : jobChunks) {
            Job mapJob = new Job(mapReduceJobID, internalJobID.get(), null, mrj.getMapper(), chunk, null);
            mapJob.reducerInterface = mrj.getReducer();
            mapJobs.add(mapJob);
            internalJobID.incrementAndGet();
        }
        int numTotalJobs = mapJobs.size() * 2 - 1;
        mrJobSuccesses.put(mapReduceJobID, new Pair(numTotalJobs,new Stack<Job>()));

        jobQueue.initializeMapJobs(mapReduceJobID, mapJobs);

        //boolean status = jobQueue.addJobs(mapJobs);
        //return status;
    }

    public static void main(String[] args) throws IOException {
        Set<File> files = new HashSet<File>();
        Set<Host> slaves = new HashSet<Host>();
        //files.add(new File("./testfile1.txt"));
        //files.add(new File("./testfile2.txt"));
        //files.add(new File("./testfile3.txt"));
        //files.add(new File("./testfile4.txt"));
        //files.add(new File("./testfile5.txt"));
        //files.add(new File("./wordcounttest.txt"));
        //files.add(new File("./floatyolotest.txt"));
        //files.add(new File("./reducewc1.txt"));
        //files.add(new File("./reducewc2.txt"));
        files.add(new File("wordcount.txt"));
        files.add(new File("primes.txt"));
        slaves.add(new Host("UNIX2.ANDREW.CMU.EDU", 6666));
        //slaves.add(new Host("UNIX3.ANDREW.CMU.EDU", 6666));
        slaves.add(new Host("UNIX4.ANDREW.CMU.EDU", 6666));
        Master master = new Master(files, slaves);
        master.listenInput();
    }

}