package com.nzb.consumerconfirm;/**
 * Created by M on 2018/1/27.
 */

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

/**
 * @author M
 * @create 2018/1/27
 */
public class ClientConsumerACK {
    private static final String EXCHANGE_NAME = "direct_cc_confirm_1";

    public static void main(String[] args) throws IOException, TimeoutException {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        String queueName = "consumer_confirm";
        channel.queueDeclare(queueName, false, false, false, null);
        String server = "error";
        channel.queueBind(queueName, EXCHANGE_NAME, server);
        System.out.println("Waiting message......");

        Consumer consumerB = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consmerTag, Envelope envelop,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Accept: " + envelop.getRoutingKey() + ":" + message);
                this.getChannel().basicAck(envelop.getDeliveryTag(), false);
            }
        };

        channel.basicConsume(queueName, false, consumerB);
    }
}