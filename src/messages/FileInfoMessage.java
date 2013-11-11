package messages;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 1:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class FileInfoMessage extends Message {

    private String fileName;
    private long fileSize; //size in bytes

    public FileInfoMessage(String fileName, long fileSize) {
        this.fileName = fileName;
        this.fileSize = fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public String toString() {
        return "[FileInfoMessage: (" + fileName + "), (" + fileSize + ")]";
    }
}
