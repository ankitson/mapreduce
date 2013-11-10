package messages;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 1:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class FileNameMessage extends Message {

    private String fileName;
    public FileNameMessage(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }
}
