package messages;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/13/13
 * Time: 7:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class HostNameMessage extends Message {
    private String hostName;
    public HostNameMessage(String hostName) {
        this.hostName = hostName;
    }

    public String getHostName() {
        return hostName;
    }
}
