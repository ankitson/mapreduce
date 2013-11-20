package master;

import Config.Configuration;
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

    private int HEALTH_CHECK_FREQUENCY; //in ms
    private ConcurrentHashMap<Host, SocketMessenger> messengers;
    private Configuration config;
    public HealthCheckerThread(ConcurrentHashMap<Host, SocketMessenger> messengers, Configuration config) {
        this.messengers = messengers;
        this.config = config;
        this.HEALTH_CHECK_FREQUENCY = config.HEALTH_CHECK_FREQUENCY;
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
