import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.nio.charset.StandardCharsets;

public class ClientWriter {
    private final static String EXCHANGE_NAME = "file_text_exchange";

    public static void main(String[] argv) throws Exception {
        // Établir une connexion à RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // Adresse du broker RabbitMQ
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // Déclarer un échange (exchange) de type fanout
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            // Message à envoyer (ligne de texte à ajouter)
            String message = "Message test";

            // Publier le message sur l'échange en mode fanout
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}
