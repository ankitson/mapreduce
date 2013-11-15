package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/10/13
 * Time: 6:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileUtils {

    public static String print(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;

        StringBuilder contents = new StringBuilder();
        while ((line = br.readLine()) != null) {
            contents.append("\n"+line);
        }
        return contents.toString();
    }

    public static void createDirectory(String directory) {
        File dir = new File(directory);
        if(!dir.exists()) {
            dir.mkdirs();
        }
        return;
    }

    //create a new file, creating the necessary directory structure if
    //the dirs do not exist.
    public static void createFile(File path) throws IOException {
        if (!path.exists()) {
            File parentDir = path.getParentFile();

            if (!parentDir.exists()) {
                parentDir.mkdirs();
            }
            path.createNewFile();
        } else {
            path.delete();
        }
    }

    public static int countLines(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        int count = 0;
        while (br.readLine() != null) {
            count++;
        }
        return count;
    }
}
