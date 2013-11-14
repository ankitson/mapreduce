package slave;

import dfs.Chunk;
import jobs.Job;
import jobs.ReducerInterface;
import messages.FileInfoMessage;
import messages.JobMessage;
import messages.SocketMessenger;
import util.FileUtils;
import util.Pair;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/12/13
 * Time: 4:06 AM
 * To change this template use File | Settings | File Templates.
 */
public class ReduceJobServicerThread extends JobThread {

    public String REDUCE_OUT_FORMAT_STRING = "%s:%s\n";
    Job reduceJob;
    SocketMessenger masterMessenger;
    String hostName;
    Chunk chunk1;
    Chunk chunk2;
    ReducerInterface reducer;

    public ReduceJobServicerThread(Job reduceJob, SocketMessenger masterMessenger, String hostName) {
        this.reduceJob = reduceJob;
        this.masterMessenger = masterMessenger;
        this.hostName = hostName;
        chunk1 = reduceJob.chunk1;
        chunk2 = reduceJob.chunk2;
        reducer = reduceJob.reducerInterface;
    }

    public void run() {
        System.out.println("Reduce thread running");
        File chunk1File = getFileFromChunk(chunk1, hostName);
        File chunk2File = getFileFromChunk(chunk2, hostName);

        try {
            if (chunk1File == null || chunk2File == null) {
                failJob();
                return;
            }

            BufferedReader chunk1Reader = new BufferedReader(new FileReader(chunk1File));
            BufferedReader chunk2Reader = new BufferedReader(new FileReader(chunk2File));
            String line;

            File reduceOutputFile = new File(Chunk.getPathPrefixOnHost(hostName) + getOutputFileName());
            FileUtils.createFile(reduceOutputFile);
            BufferedWriter reduceOutWriter = new BufferedWriter(new FileWriter(reduceOutputFile));

            Map<String,String> reducedKVs = new HashMap<String,String>();
            while ((line = chunk1Reader.readLine()) != null) {
                line = line.trim();
                String[] split = line.split(":");
                String key = split[0];
                String val = split[1];
                if (reducedKVs.containsKey(key)) {
                    String existingV = reducedKVs.get(key);
                    Pair<String,String> kv1 = new Pair<String,String>(key,val);
                    Pair<String,String> kv2 = new Pair<String,String>(key,existingV);
                    System.out.println("reduce kv1: " + kv1);
                    System.out.println("reduce kv2: " + kv2);
                    Pair<String,String> reducedKV = reducer.reduce(kv1,kv2);
                    reducedKVs.put(reducedKV.getFirst(),reducedKV.getSecond().toString());
                }
                reducedKVs.put(key,val);
            }

            while ((line = chunk2Reader.readLine()) != null) {
                line = line.trim();
                String[] split = line.split(":");
                String key2 = split[0];
                String val2 = split[1];
                if (reducedKVs.containsKey(key2)) {
                    String key1 = key2;
                    String val1 = reducedKVs.get(key2);
                    Pair<String,String> kv1 = new Pair<String,String>(key1,val1);
                    Pair<String,String> kv2 = new Pair<String,String>(key2,val2);
                    System.out.println("reduce kv1: " + kv1);
                    System.out.println("reduce kv2: " + kv2);
                    Pair<String,String> reducedKV = reducer.reduce(kv1,kv2);
                    reducedKVs.put(reducedKV.getFirst(),reducedKV.getSecond());
                }
            }

            for (Map.Entry<String,String> entry : reducedKVs.entrySet()) {
                reduceOutWriter.write(String.format(REDUCE_OUT_FORMAT_STRING, entry.getKey(), entry.getValue()));
            }

            reduceOutWriter.close();
            successJob(reduceOutputFile);
        } catch (IOException e) {
            System.err.println("Reduce thread unable to communicate");
            return;
        }
    }

    public void failJob() throws IOException {
        reduceJob.success = false;
        masterMessenger.sendMessage(new JobMessage(reduceJob));
        return;
    }

    public void successJob(File file) throws IOException {
        reduceJob.success = true;
        masterMessenger.sendMessage(new JobMessage(reduceJob));
        masterMessenger.sendMessage(new FileInfoMessage(file.getName(),file.length()));
        masterMessenger.sendFile(file);
    }

    public String getOutputFileName() {
        return String.format("map.%d.%s-%s.%s-%s", reduceJob.internalJobID, chunk1.getFileName(), chunk1.getChunkNo()
                                                 , chunk2.getFileName(), chunk2.getChunkNo());
    }
}
