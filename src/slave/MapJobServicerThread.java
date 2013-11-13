package slave;

import dfs.Chunk;
import jobs.Job;
import jobs.MapperInterface;
import messages.*;
import util.FileUtils;
import util.Host;
import util.Pair;

import java.io.*;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/12/13
 * Time: 4:04 AM
 * To change this template use File | Settings | File Templates.
 */

//TODO:
//take care of record range & combiner
public class MapJobServicerThread implements Runnable {

    private static String OUTPUT_FORMAT_STRING = "%s:%s\n";

    Job mapJob;
    SocketMessenger masterMessenger;
    Chunk chunk;
    MapperInterface mapper;
    String hostName;

    public MapJobServicerThread(Job mapJob, SocketMessenger masterMessenger, String hostName) {
        this.mapJob = mapJob;
        this.masterMessenger = masterMessenger;
        this.hostName = hostName;
        chunk = mapJob.chunk;
        mapper = mapJob.mapperInterface;
    }

    public void run() {
        System.out.println("Map thread running");

        File mapInputFile = null;
        try {
            File chunkPathFile = new File(chunk.getPathOnHost(hostName));
            System.out.println("local chunk path: " + chunkPathFile.getCanonicalPath());
            mapInputFile = chunkPathFile;
            if (!chunkPathFile.exists()) { //the chunk is on a different slave
                System.out.println("chunk does NOT exist locally ");
                for (Host host : chunk.getHosts()) {
                    try {
                        System.out.println("requesting " + host + " for chunk");
                        Socket socket = new Socket(host.HOSTNAME, 5358);
                        SocketMessenger hostMessenger = new SocketMessenger(socket);
                        hostMessenger.sendMessage(new ChunkMessage(chunk, host.HOSTNAME));
                        Message message = hostMessenger.receiveMessage();
                        if (message instanceof FileInfoMessage) {
                            FileInfoMessage fim = ((FileInfoMessage) message);
                            File receivedFile = new File(Chunk.CHUNK_PATH + fim.getFileName());
                            hostMessenger.receiveFile(receivedFile, (int) fim.getFileSize());
                            mapInputFile = receivedFile;
                        } else if (message instanceof FileNotFoundMessage) {
                            continue;
                        } else {
                            System.err.println("Unknown message received in map servicer");
                            failJob();
                        }
                        socket.close();
                    } catch (IOException e) {
                        System.err.println("IO Exception in map servicer: " + e);
                        failJob();
                    } catch (ClassNotFoundException e) {
                        System.err.println("Class not found in map servicer: " + e);
                        failJob();
                    }
                }
            }
            if (mapInputFile == null) {
                System.err.println("Unable to find required chunk on any slave");
                failJob();
            }
            BufferedReader br = new BufferedReader(new FileReader(mapInputFile));
            System.out.println("input file contents: " + FileUtils.print(mapInputFile));

            //output of map is stored in chunk dir
            File mapOutputFile = new File(Chunk.CHUNK_PATH + getOutputFileName());
            FileUtils.createFile(mapOutputFile);
            System.out.println("created output file: " + mapOutputFile.getCanonicalPath());
            BufferedWriter outputWriter = new BufferedWriter(new FileWriter(mapOutputFile));

            String line;
            int recordNo = chunk.getRecordRange().getFirst();
            try {
                while ((line = br.readLine()) != null) {
                    System.out.println("read " + line);
                    Pair output = mapper.map(line,recordNo);
                    outputWriter.write(String.format(OUTPUT_FORMAT_STRING, output.getFirst(), output.getSecond()));
                    System.out.println("writing: " + output);
                    recordNo++;
                }
            } catch (IOException e) {
                System.err.println("Unable to write to map output file: " + e);
                failJob();
            }
            outputWriter.close();
            successJob(getOutputFileName());
        } catch (IOException e) {
            System.err.println("Thread unable to communicate");
            return;
        }

        //send job message after everything

        /*try {
            Thread.sleep(10000);
            System.out.println("requesting now");

            Set<Host> chunkRequestHosts = new HashSet<Host>();
            chunkRequestHosts.add(new Host("UNIX3.ANDREW.CMU.EDU", 5358));
            chunkRequestHosts.add(new Host("UNIX4.ANDREW.CMU.EDU", 5358));
            Chunk chunk = new Chunk("testfile2.txt", 2, chunkRequestHosts);

            for (Host host : chunkRequestHosts) {
                SocketMessenger messenger1 = new SocketMessenger(host.getSocket());
                messenger1.sendMessage(new ChunkMessage(chunk, host.HOSTNAME));
                System.out.println("requested chunk from: " + host);
                try {
                    Message received = messenger1.receiveMessage();
                    if (received instanceof FileInfoMessage) {
                        FileInfoMessage fim = ((FileInfoMessage) received);
                        File receivedFile = new File(fim.getFileName());
                        messenger1.receiveFile(receivedFile, (int) fim.getFileSize());
                        System.out.println("received file: " + FileUtils.print(receivedFile));
                    } else if (received instanceof FileNotFoundMessage) {
                        System.err.println("chunk not found");
                    }
                } catch (ClassNotFoundException e) {
                    System.err.println("illegal message in chunk test");
                }
            }
        } catch (IOException e) {
            System.err.println("exception requesting chunk: " + e);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }*/
    }

    public void failJob() throws IOException {
        mapJob.success = false;
        masterMessenger.sendMessage(new JobMessage(mapJob));
        return;
    }

    public void successJob(String outFileName) throws IOException {
        mapJob.success = true;
        masterMessenger.sendMessage(new JobMessage(mapJob, outFileName));
        return;
    }

    public String getOutputFileName() {
        return String.format("map.%d.%s-%s", mapJob.internalJobID, chunk.getFileName(), chunk.getChunkNo());
    }

}
