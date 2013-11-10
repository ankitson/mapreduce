package config;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/9/13
 * Time: 8:58 PM
 * To change this template use File | Settings | File Templates.
 */

import java.io.*;
import java.util.*;

/**
 * A configuration is a set of <key,value> strings, which is represented
 * in memory as a map from string to lists of strings, and on disk as:
 * KEY1=VALUE1
 * KEY2=VALUE2,VALUE3,VALUE4
 *
 */
public class ConfigUtil {

    /**
     * Parses a config file into a Map.
     * Lines not of the form KEY=VALUE are ignored.
     */
    public static Map<String,List<String>> readConfig(File configFile) throws FileNotFoundException {
        Map<String,List<String>> configObject = new HashMap<String,List<String>>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(configFile));
            String line;
            while ((line = br.readLine()) != null) {
                String[] components = line.split("=");
                if (components.length != 2)
                    continue;
                List<String> values = new ArrayList<String>();
                String[] valuesString = components[1].split(",");
                for (int i=0;i<valuesString.length;i++) {
                    System.out.println("valuesString[i]: " + valuesString[i].toString());
                    values.add(valuesString[i].toString());
                }
                configObject.put(components[0],values);
            }
            br.close();
        }
        catch (IOException e) {
            return null;
        }
        return configObject;
    }

    public static boolean writeConfig(Map<String,List<String>> configObject, File outFile) {
        if (!outFile.exists()) {
            try {
                outFile.createNewFile();
            } catch (IOException e) {
                System.err.println("Error creating out file: " + e);
            }
        }
        try {
            BufferedWriter output = new BufferedWriter(new FileWriter(outFile));
            for (Map.Entry<String, List<String>> entry : configObject.entrySet()) {
                output.write(entry.getKey() + "=" + entry.getValue() + "\n");
            }
            output.close();
        }
        catch (IOException e) {
            System.err.println("Error writing config file: " + e);
            return false;
        }
        return true;
    }

    public static void main(String[] args) throws FileNotFoundException {
        Map<String, List<String>> testConfig = new HashMap<String, List<String>>();
        testConfig.put("ips", Arrays.asList("1", "2"));
        testConfig.put("bla",Arrays.asList("blargh"));
        ConfigUtil.writeConfig(testConfig, new File("./configtest.txt"));
        Map<String, List<String>> readConfig = ConfigUtil.readConfig(new File("./configtest.txt"));
        System.out.println("read Config = test Config: " + readConfig.equals(testConfig));
        System.out.println("read config: " + readConfig);
        System.out.println("testConfig: " + testConfig);
    }
}
