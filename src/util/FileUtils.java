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
}
