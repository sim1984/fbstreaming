package com.hqbird.fbstreaming.examples.rabbitmq.receiver;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class Main {

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

    public static void main(String[] argv) throws Exception {
        Properties properties = getProperties();
        final String rabbitHost = properties.getProperty("rabbit.host");
        final String queueName = properties.getProperty("rabbit.queueName");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitHost);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(queueName, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}
