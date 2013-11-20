package slave;

import messages.MapReduceMessage;
import messages.SocketMessenger;

import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * User: Adi
 * Date: 11/20/13
 * Time: 12:00 AM
 * To change this template use File | Settings | File Templates.
 */
public class TaskInstantiaterThread implements Runnable {

    private SocketMessenger messenger;

    public  TaskInstantiaterThread(SocketMessenger messenger) {
        this.messenger = messenger;
    }

    public void run() {

        Scanner in = new Scanner(System.in);
        while (true) {
            String line = in.nextLine();
            if (line.startsWith("run"))   {
                String[] args = line.split(" ");
                if (args.length < 2) {
                    System.out.println("invalid input");
                }
                String className = args[1];
                MapReduceMessage msg = new MapReduceMessage();
                msg.className = className;
                try {
                    this.messenger.sendMessage(msg);
                } catch (Exception e) {
                    System.err.println("Cannot send Master the MapReduceMessage");
                }
            }
        }

    }
}
