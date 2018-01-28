package com.nzb.consumerconfirm;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author M
 * @create 2018/1/27
 */
public class ClientConsumerReject {
    private static final String EXCHANGE_NAME = "direct_cc_confirm_1";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
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
                this.getChannel().basicReject(envelop.getDeliveryTag(), true);
                System.out.println("Reject: " + envelop.getRoutingKey() + ":" + new String(body, "UTF-8"));
            }
        };

        channel.basicConsume(queueName, false, consumerB);
    }
}
