import com.rabbitmq.client.*;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class ClientReader {
    private static final String QUEUE_NAME = "reader_queue";
    private final static String EXCHANGE_NAME = "read_exchange";


    public static void main(String[] argv) throws Exception {

        readLast();
        processRead();

    }

    private static void processRead() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // Adresse du broker RabbitMQ
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // Declare a queue for receiving the responses
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.exchangeDeclare("direct_read", BuiltinExchangeType.DIRECT);
        // Bind the queue to the exchange
        channel.queueBind(QUEUE_NAME, "direct_read", "");

        System.out.println(" [Client reader] Waiting for messages. To exit press CTRL+C");

        // Set up a consumer to consume messages from the queue
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [Client reader] Received '" + message + "'");
        };

        // Consume messages from the queue
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });
    }

    private static void readLast() throws Exception {
        // Établir une connexion à RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // Adresse du broker RabbitMQ
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            // Déclarer un échange (exchange) de type fanout
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            // Message à envoyer (ligne de texte à ajouter)
            String message = "Read Last";

            // Publier le message sur l'échange en mode fanout
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [Client reader] Sent '" + message + "'");
        }
    }
}