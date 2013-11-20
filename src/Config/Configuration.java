package Config;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Adi
 * Date: 11/19/13
 * Time: 9:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class Configuration {

    public int NUMBER_OF_SLAVES;
    public List<String> SLAVE_HOSTNAMES;
    public String MASTER_HOSTNAME;
    public int MASTER_SLAVE_PORT;
    public int SLAVE_SLAVE_PORT;
    public int REPLICATION_FACTOR;
    public int CHUNK_SIZE;
    public int HEALTH_CHECK_FREQUENCY;
    public int TRIES;
    public String CACHE_FILE_DEFAULT_DIRECTORY;
    public int CACHE_SIZE;

    public Configuration(String filepath) throws IOException {
        this.setParameters(filepath);
    }

    public void setParameters(String file) throws IOException {

        FileInputStream fstream = new FileInputStream(file);
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));


        String line;
        while ((line = br.readLine()) != null) {
            String[] split = line.split(":");

            if (split[0].equals("NUMBER_OF_SLAVES"))
                this.NUMBER_OF_SLAVES = Integer.parseInt(split[1]);
            else if (split[0].equals("MASTER_SLAVE_PORT"))
                this.MASTER_SLAVE_PORT = Integer.parseInt(split[1]);
            else if (split[0].equals("SLAVE_SLAVE_PORT"))
                this.SLAVE_SLAVE_PORT = Integer.parseInt(split[1]);
            else if (split[0].equals("CHUNK_SIZE"))
                this.CHUNK_SIZE = Integer.parseInt(split[1]);
            else if (split[0].equals("REPLICATION_FACTOR"))
                this.REPLICATION_FACTOR = Integer.parseInt(split[1]);
            else if (split[0].equals("MASTER_HOSTNAME"))
                this.MASTER_HOSTNAME = split[1];
            else if (split[0].equals("HEALTH_CHECK_FREQUENCY"))
                this.HEALTH_CHECK_FREQUENCY = Integer.parseInt(split[1]);
            else if (split[0].equals("TRIES"))
                this.TRIES = Integer.parseInt(split[1]);
            else if (split[0].equals("CACHE_SIZE"))
                this.CACHE_SIZE = Integer.parseInt(split[1]);
            else if (split[0].equals("CACHE_FILE_DEFAULT_DIRECTORY"))
                this.CACHE_FILE_DEFAULT_DIRECTORY = split[1];
            else if (split[0].equals("SLAVE_HOSTNAMES")) {
                this.SLAVE_HOSTNAMES = new ArrayList<String>();
                String[] slaves =   split[1].split(",");
                for (int i = 0; i < slaves.length; i++) {
                    this.SLAVE_HOSTNAMES.add(slaves[i]);
                }
            }
        }
        br.close();

    }

    public String toString() {
        return "NUMBER_OF_SLAVES = " + this.NUMBER_OF_SLAVES +
                "\nMASTER_SLAVE_PORT = " + this.MASTER_SLAVE_PORT +
                "\nSLAVE_SLAVE_PORT = " + this.SLAVE_SLAVE_PORT +
                "\nREPLICATION_FACTOR = " + this.REPLICATION_FACTOR +
                "\nCHUNK_SIZE = " + this.CHUNK_SIZE +
                "\nHEALTH_CHECK_FREQUENCY = " + this.HEALTH_CHECK_FREQUENCY +
                "\nTRIES = " + this.TRIES +
                "\nSLAVE_HOSTNAMES = " + this.SLAVE_HOSTNAMES +
                "\nMASTER_HOSTNAME = " + this.MASTER_HOSTNAME +
                "\nCACHE_SIZE = " + this.CACHE_SIZE +
                "\nCACHE_FILE_DEFAULT_DIRECTORY = " + this.CACHE_FILE_DEFAULT_DIRECTORY;
    }

}
