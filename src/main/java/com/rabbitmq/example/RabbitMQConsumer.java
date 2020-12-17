package com.rabbitmq.example;

import java.util.ArrayList;

import com.rabbitmq.client.*;
public class RabbitMQConsumer {
  public static void main(String []args) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setVirtualHost("/");
    factory.setHost("127.0.0.1");
    factory.setPort(5672);
    Connection conn = factory.newConnection();
    Channel channel = conn.createChannel();

    String exchangeName = "myExchange";
    String routingKey = "testRoute";
    String queueName = "testQueueName";
    //1个channel 1个 thread
    channel.exchangeDeclare(exchangeName, "direct", true);
    channel.queueDeclare(queueName, true, false, false, null);
    channel.queueBind(queueName, exchangeName, routingKey);

    boolean autoAck = false;

    boolean runInfinite = true;
    long lcount =0;
    while (runInfinite) {
    	System.out.print("pull msg "+lcount+++" ");
        GetResponse response = channel.basicGet(queueName, autoAck);
        ArrayList<Long> al=new ArrayList<Long>();
        if (response == null) {
            // No message retrieved.
        } else {
        	
            AMQP.BasicProperties props = response.getProps();
            byte[] body = response.getBody();
            long deliveryTag = response.getEnvelope().getDeliveryTag();
            System.out.println("Message received: [" + deliveryTag + ", "+ response.getProps().getAppId()+"] " + new String(body));
            Thread.currentThread().sleep(3);
            al.add(deliveryTag);
        }
        if(al.size()==100) {
        	Thread.currentThread().sleep(300);
        	for (Long long1 : al) {
        		channel.basicAck(long1, false);
        		al.remove(long1);
			}
        }

    }
    channel.close();
    conn.close();
  }
} 