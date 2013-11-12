package config;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: ankit
 * Date: 11/11/13
 * Time: 7:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class Configuration {

    public Map<String, List<String>> configMap;
    public Configuration(File configFile) throws FileNotFoundException {
        configMap = ConfigUtil.readConfig(configFile);
    }
}
