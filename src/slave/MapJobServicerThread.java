package slave;

import dfs.Chunk;
import messages.*;
import util.FileUtils;
import util.Host;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/12/13
 * Time: 4:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class MapJobServicerThread implements Runnable {

    public void run() {
        System.out.println("Map thread running");

        try {
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
        }
    }
}
