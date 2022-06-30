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
import java.util.logging.Logger;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class StreamingApp {
    static final Logger logger = Logger.getLogger(StreamingApp.class.getName());

    /**
     * Получение настроек по умолчанию из ресурсов
     *
     * @return настройки
     * @throws IOException ошибка загрузки настроек
     */
    public static Properties getDefaultProperties() throws IOException {
        try (InputStream input = StreamingApp.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            return prop;
        }
    }

    /**
     * Получение настроек из файла конфигурации
     *
     * @return настройки
     * @throws IOException ошибка загрузки настроек
     */
    public static Properties getProperties() throws IOException {
        try (InputStream input = new FileInputStream("./config.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            return prop;
        } catch (IOException e) {
            return getDefaultProperties();
        }
    }

    /**
     * Загружаем плагин по имени
     *
     * @param className полное имя класса плагина
     * @return экземпляр плагина
     */
    public static FbStreamPlugin loadPlugin(String className) throws Exception {
        File pluginDir = new File("plugins");

        // папке с плагинами ищем все файлы с расширением .jar
        File[] jars = pluginDir.listFiles(file -> file.isFile() && file.getName().endsWith(".jar"));
        // составляем список url доя jar файлов
        if (jars == null) jars = new File[0];
        List<URL> list = new ArrayList<>();
        for (File jar : jars) {
            URL url = jar.toURI().toURL();
            list.add(url);
        }
        URL[] urls = list.toArray(new URL[0]);
        // инициализируем загрузчик классов из списка url
        URLClassLoader loader = new URLClassLoader(urls);
        // загружаем и создаём экземпляр плагина
        Class<?> class_ = loader.loadClass(className);
        return (FbStreamPlugin) class_.newInstance();
    }

    public static void main(String[] args) throws Exception {
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        Properties properties = getProperties();

        // загружаем плагин по имени класса из настроек
        FbStreamPlugin plugin = loadPlugin(properties.getProperty("pluginClassName"));
        // запускаем его первый раз
        plugin.invoke(properties);

        WatchService watchService = FileSystems.getDefault().newWatchService();
        Path path = Paths.get(properties.getProperty("incomingFolder"));
        //будем следить за созданием новых файлов в папке с журналами репликации
        path.register(watchService, ENTRY_CREATE);
        boolean poll = true;
        while (poll) {
            WatchKey key = watchService.take();
            for (WatchEvent<?> event : key.pollEvents()) {
                // когда новый файл появляется запускаем плагин
                plugin.invoke(properties);
            }
            poll = key.reset();
        }
    }
}
