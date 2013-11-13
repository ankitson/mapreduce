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

    private int HEALTH_CHECK_FREQUENCY = 10000; //in ms
    private ConcurrentHashMap<Host, SocketMessenger> messengers;
    public HealthCheckerThread(ConcurrentHashMap<Host, SocketMessenger> messengers) {
        this.messengers = messengers;
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(HEALTH_CHECK_FREQUENCY);
                for (SocketMessenger messenger : messengers.values()) {
                    messenger.sendMessage(new HeartBeatMessage());
                }
            } catch (InterruptedException e) {
                System.err.println("Health checker thread interrupted");
            } catch (IOException e) {
                System.err.println("Unable to send heartbeat message");
            }
        }
    }


}
