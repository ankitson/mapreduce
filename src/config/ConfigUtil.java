package config;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/9/13
 * Time: 8:58 PM
 * To change this template use File | Settings | File Templates.
 */

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * A configuration is a set of <key,value> strings, which is represented
 * in memory as a map, and on disk as a file with format:
 * KEY1=VALUE1
 * KEY2=VALUE2
 */
public class ConfigUtil {

    /**
     * Parses a config file into a Map.
     * Lines not of the form KEY=VALUE are ignored.
     */
    public static Map<String,String> readConfig(File configFile) throws FileNotFoundException {
        Map<String,String> configObject = new HashMap<String,String>();
        try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] components = line.split("=");
                if (components.length != 2)
                    continue;
                configObject.put(components[0],components[1]);
            }
        }
        catch (IOException e) {
            return null;
        }
        return configObject;
    }

    public static boolean writeConfig(Map<String,String> configObject, File outFile) {
        try (BufferedWriter output = new BufferedWriter(new FileWriter(outFile))) {
            for (Map.Entry<String, String> entry : configObject.entrySet()) {
                output.write(entry.getKey()+"="+entry.getValue());
            }
        }
        catch (IOException e) {
            System.err.println("Error writing config file");
            return false;
        }
        return true;
    }
}
