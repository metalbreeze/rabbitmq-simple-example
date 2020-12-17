package com.rabbitmq.example;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.*;

public class RabbitMQProducer {
  public static void main(String []args) throws Exception {

    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setVirtualHost("/");
    factory.setHost("127.0.0.1");
    factory.setPort(5672);
    Connection conn = factory.newConnection();
    Channel channel = conn.createChannel();
//https://www.rabbitmq.com/api-guide.html
    String exchangeName = "myExchange";
    String routingKey = "testRoute";
    String queueName = "testQueueName";
    //1个channel 1个 thread
    channel.exchangeDeclare(exchangeName, "direct", true);
    channel.queueDeclare(queueName, true, false, false, null);
    channel.queueBind(queueName, exchangeName, routingKey);
    
    int count = 1;
    if (args.length == 1) {
      count = Integer.parseInt(args[0]);
    }
    for (int i = 0; i < count; i++) {
      byte[] messageBodyBytes = ("Hello["+ (i + 1) + "], world!").getBytes();
      channel.basicPublish(exchangeName, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes);
      System.out.println("publish"+i);
// 是不是下面更好?
//      channel.basicPublish(exchangeName, routingKey,
//              new AMQP.BasicProperties.Builder()
//                .contentType("text/plain")
//                .deliveryMode(2)
//                .priority(1)
//                .userId("bob")
//                .build(),
//                messageBodyBytes);
    }

    channel.close();
    conn.close();
  }
}