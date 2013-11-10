package messages;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 1:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class FileMessage extends Message {

    private File file;
    public FileMessage(File file) {
        this.file = file;
    }

    public File getFile() {
        return file;
    }
}
