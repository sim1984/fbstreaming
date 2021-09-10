package com.hqbird.fbstreaming;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class StreamingApp {
    static final Logger logger = Logger.getLogger(StreamingApp.class.getName());

    public static Properties getDefaultProperties() throws IOException {
        try (InputStream input = StreamingApp.class.getClassLoader().getResourceAsStream("config.properties")) {
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

    public static FbStreamPlugin loadPlugin(String className) throws Exception {
        File pluginDir = new File("plugins");

        File[] jars = pluginDir.listFiles(file -> file.isFile() && file.getName().endsWith(".jar"));
        if (jars == null) jars = new File[0];
        List<URL> list = new ArrayList<>();
        for (File jar : jars) {
            URL url = jar.toURI().toURL();
            list.add(url);
        }
        URL[] urls = list.toArray(new URL[0]);
        URLClassLoader loader = new URLClassLoader(urls);
        Class<?> class_ = loader.loadClass(className);
        return (FbStreamPlugin) class_.newInstance();
    }

    public static void main(String[] args) throws Exception {
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        Properties properties = getProperties();

        FbStreamPlugin plugin = loadPlugin(properties.getProperty("pluginClassName"));
        plugin.invoke(properties);

        WatchService watchService = FileSystems.getDefault().newWatchService();
        Path path = Paths.get(properties.getProperty("incomingFolder"));
        //будем следить за созданием, изменение и удалением файлов.
        path.register(watchService, ENTRY_CREATE);
        boolean poll = true;
        while (poll) {
            WatchKey key = watchService.take();
            for (WatchEvent<?> event : key.pollEvents()) {
                plugin.invoke(properties);
            }
            poll = key.reset();
        }
    }
}
