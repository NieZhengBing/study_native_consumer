package com.nzb.normal;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

/**
 * @author M
 * @create 2018/1/27
 */
public class ConsumerAll {
    private static final String EXCHANGE_NAME = "fanout_logs_1";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        String queueName = channel.queueDeclare().getQueue();
        String[] serverties = {"error", "info", "warning"};
        for (String server : serverties) {
            channel.queueBind(queueName, EXCHANGE_NAME, server);
        }
        System.out.println("Waiting message......");

        Consumer consumerA = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws UnsupportedEncodingException {
                String message = new String(body, "UTF-8");
                System.out.println("Accept: " + envelope.getRoutingKey() + ":" + message);
            }
        };

        channel.basicConsume(queueName, true, consumerA);
    }
}
