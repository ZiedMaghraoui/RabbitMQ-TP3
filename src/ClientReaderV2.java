import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ClientReaderV2 {
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

    private static void processRead(Channel channel) throws IOException, InterruptedException {
        // initialize a list of strings:
        List<String> messagesList = new ArrayList<String>();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
        // Set the timeout in milliseconds
        int timeoutMillis = 5000; // 5 seconds

        // Start the timer
        long startTime = System.currentTimeMillis();
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            messagesList.add( message );

            System.out.println(" [*] Received '" + message + "'");
          //  System.out.println("message List:"+ messagesList);
            if(!(System.currentTimeMillis() - startTime < timeoutMillis))
                if(messagesList.get(0).equals(messagesList.get(1)) ) 
                    System.out.println("The last message is: " + messagesList.get(0));
                else {
                    System.out.println("The last message is: " + messagesList.get(2));
                }
        };
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });
        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            Thread.sleep(100); // Sleep for a short interval
        }
        System.out.println("Consuming stopped due to timeout or all messages consumed.");
        messagesList.sort(null);
        System.out.println("The last message is: " + messagesList.get(messagesList.size()-1));
        System.exit(0);

    }

    private static void requestReadLast(Channel channel) throws Exception {
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        String message = "Read Last";
        channel.basicPublish(EXCHANGE_NAME, Replica.ROUTING_KEY_READER , null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [*] Sent '" + message + "'");
    }
}