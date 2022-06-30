package com.hqbird.fbstreaming.examples.lucenefts;

import com.hqbird.fbstreaming.FbStreamPlugin;
import com.hqbird.fbstreaming.plugin.ftslucene.FTSLuceneStreamPlugin;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    static final Logger logger = Logger.getLogger(Main.class.getName());

    public static Properties getDefaultProperties() throws IOException {
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            return prop;
        }
    }

    public static Properties getProperties() throws IOException {
        try (InputStream input = new FileInputStream("./config.properties")) {
            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            return prop;
        } catch (IOException e) {
            return getDefaultProperties();
        }
    }

    public static void main(String[] args) throws Exception {
        Properties properties = getProperties();

        FbStreamPlugin plugin = new FTSLuceneStreamPlugin();
        int howManyWasSent = plugin.invoke(properties);

        logger.log(Level.INFO, "Files processed: " + howManyWasSent);

        System.exit(1);

    }
}
