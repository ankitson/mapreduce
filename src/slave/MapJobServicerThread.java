package slave;

import dfs.Chunk;
import jobs.Job;
import jobs.KVContainer;
import jobs.MapperInterface;
import jobs.ReducerInterface;
import messages.JobMessage;
import messages.SocketMessenger;
import util.FileUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/12/13
 * Time: 4:04 AM
 * To change this template use File | Settings | File Templates.
 */

//TODO:
//take care of record range & combiner
public class MapJobServicerThread extends JobThread {

    public static String OUTPUT_FORMAT_STRING = "%s:%s\n";

    Job mapJob;
    SocketMessenger masterMessenger;
    Chunk chunk;
    MapperInterface mapper;
    String hostName;
    ReducerInterface combiner;

    public MapJobServicerThread(Job mapJob, SocketMessenger masterMessenger, String hostName) {
        this.mapJob = mapJob;
        this.masterMessenger = masterMessenger;
        this.hostName = hostName;
        chunk = mapJob.chunk;
        mapper = mapJob.mapperInterface;
        combiner = mapper.getCombiner();
        System.out.println("combiner in map job svc thread: " + combiner);
    }

    public void run() {
        System.out.println("Map thread running");

        File mapInputFile = null;
        try {
            mapInputFile = getFileFromChunk(chunk, hostName);

            /*File chunkPathFile = new File(chunk.getPathOnHost(hostName));
            System.out.println("local chunk path: " + chunkPathFile.getCanonicalPath());
            mapInputFile = chunkPathFile;

            if (!chunkPathFile.exists()) //the chunk is on a different slave
                mapInputFile = getFileFromChunk(chunk);*/

            if (mapInputFile == null) {
                System.err.println("Unable to find required chunk on any slave");
                failJob();
            }
            BufferedReader br = new BufferedReader(new FileReader(mapInputFile));
            System.out.println("input file contents: " + FileUtils.print(mapInputFile));

            //output of map is stored in chunk dir
            File mapOutputFile = new File(Chunk.getPathPrefixOnHost(hostName) + getOutputFileName());
            FileUtils.createFile(mapOutputFile);
            System.out.println("created output file: " + mapOutputFile.getCanonicalPath());
            BufferedWriter outputWriter = new BufferedWriter(new FileWriter(mapOutputFile));

            String line;
            int recordNo = chunk.getRecordRange().getFirst();

            List<KVContainer> outputKVs = new ArrayList<KVContainer>();
            try {
                try {
                    while ((line = br.readLine()) != null) {
                        KVContainer inputKV = mapper.parseRecord(line, recordNo);
                        KVContainer outputKV = new KVContainer();
                        mapper.map(inputKV.getKey(), inputKV.getValue(), outputKV);
                        outputKVs.add(outputKV);
                        //String toWrite = mapper.KVtoString(outputKV);
                        //outputWriter.write(toWrite+"\n");
                        recordNo++;
                    }
                    Collections.sort(outputKVs);
                    outputKVs = applyCombiner(outputKVs);
                    for (KVContainer outputKV : outputKVs) {
                        outputWriter.write(mapper.KVtoString(outputKV)+"\n");
                    }
                    outputWriter.close();
                } catch (IOException e) {
                    System.err.println("Unable to write map output file: " + e);
                    failJob();
                    return;
                }
                successJob(getOutputFileName());
            } catch (IOException e) {
                System.err.println("Map service thread unable to communicate: " + e);
            }
            /*List<Pair<String, String>> outputPairs = new ArrayList<Pair<String, String>>();
            try {
                while ((line = br.readLine()) != null) {
                    System.out.println("read " + line);
                    Pair output = mapper.map(line,recordNo);
                    outputPairs.add(new Pair(output.getFirst(), output.getSecond()));
                    recordNo++;
                }
                Collections.sort(outputPairs);

                Pair<String,String> lastPair = outputPairs.get(0);
                Pair combinedValue = null;

                if (combiner != null) {
                    System.out.println("comber not null");
                    for (Pair<String, String> outputPair : outputPairs.subList(1,outputPairs.size()) ) {
                        System.out.println("iterating: " + outputPair);
                        if (outputPair.getFirst().equals(lastPair.getFirst())) {
                            combinedValue = combiner.combine(lastPair, outputPair);
                            System.out.println("combining: " + outputPair + ", " + lastPair + " to " + combinedValue);
                        } else {
                            System.out.println("writing combined: " + combinedValue );
                            outputWriter.write(String.format(OUTPUT_FORMAT_STRING, lastPair.getFirst(), combinedValue.getSecond()));
                            combinedValue = null;
                        }
                        lastPair = outputPair;
                    }
                    if (combinedValue == null) {
                        outputWriter.write(String.format(OUTPUT_FORMAT_STRING, lastPair.getFirst(), lastPair.getSecond()));
                    }
                } else {
                    for (Pair pair : outputPairs) {
                        System.out.println("no combiner werite loop");
                        outputWriter.write(String.format(OUTPUT_FORMAT_STRING, pair.getFirst(), pair.getSecond()));
                    }
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
        }*/
        } catch (IOException e) {
            System.err.println(e);
        }
        return;
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

    public List<KVContainer> applyCombiner(List<KVContainer> sortedOutputKVs) {
        if (combiner == null || sortedOutputKVs.size() == 0)
            return sortedOutputKVs;

        List<KVContainer> combinedKVs = new ArrayList<KVContainer>();

        KVContainer toAddKV = sortedOutputKVs.get(0);
        for (KVContainer kvContainer : sortedOutputKVs.subList(1,sortedOutputKVs.size())) {
            if (toAddKV.equals(kvContainer)) {
                combiner.reduce(toAddKV.getKey(), kvContainer.getKey(), toAddKV.getValue(), kvContainer.getValue(), toAddKV);
            } else {
                combinedKVs.add(toAddKV);
                toAddKV = kvContainer;
            }
        }
        combinedKVs.add(toAddKV);

        return combinedKVs;
    }

}
