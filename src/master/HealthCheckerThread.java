package master;

import messages.HeartBeatMessage;
import messages.SocketMessenger;
import util.Host;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/12/13
 * Time: 9:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class HealthCheckerThread implements Runnable{

    private int HEALTH_CHECK_FREQUENCY = 50; //in ms
    private ConcurrentHashMap<Host, SocketMessenger> messengers;
    public HealthCheckerThread(ConcurrentHashMap<Host, SocketMessenger> messengers) {
        this.messengers = messengers;
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(HEALTH_CHECK_FREQUENCY);
                for (SocketMessenger messenger : messengers.values()) {
                    try {
                        messenger.sendMessage(new HeartBeatMessage());
                    } catch (IOException e) {
                        messengers.remove(messenger);
                    }
                }
            } catch (InterruptedException e) {
                System.err.println("Health checker thread interrupted");
            }
        }
    }


}
