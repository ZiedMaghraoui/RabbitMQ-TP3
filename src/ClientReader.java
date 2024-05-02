import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ClientReader {
    protected static final String QUEUE_NAME = "reader_queue";
    protected static final String ROUTING_KEY = "reader";
    protected final static String EXCHANGE_NAME = "read_exchange";

    public static void main(String[] argv) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try {
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();
            requestReadLast(channel);
            processRead(channel);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void processRead(Channel channel) throws IOException {
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [*] Received '" + message + "'");
            System.exit(0);
        };

        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });
    }

    private static void requestReadLast(Channel channel) throws Exception {
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        String message = "Read Last";
        channel.basicPublish(EXCHANGE_NAME, Replica.ROUTING_KEY_READER , null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [*] Sent '" + message + "'");
    }
}