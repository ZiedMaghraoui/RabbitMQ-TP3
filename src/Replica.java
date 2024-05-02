import com.rabbitmq.client.*;
import java.io.*;
import java.nio.charset.StandardCharsets;

public class Replica {
    protected final static String QUEUE_NAME_PREFIX = "replica_queue_";
    protected static final String ROUTING_KEY_READER = "replica.";
    private static String replicaDirectory;
    private final static String fileName = "replica_file.txt";

    public static void main(String[] argv) throws Exception {
        if (argv.length != 1) {
            System.out.println("Usage: Java Replica <replica_number>");
            System.exit(1);
        }

        int replicaNumber = Integer.parseInt(argv[0]);
        String queueName = QUEUE_NAME_PREFIX + replicaNumber;

        replicaDirectory = "replica_" + replicaNumber;

        File directory = new File(replicaDirectory);
        if (!directory.exists()) {
            directory.mkdir();
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(queueName, false, false, false, null);
        channel.queuePurge(queueName);

        channel.exchangeDeclare(ClientWriter.EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        channel.exchangeDeclare(ClientReader.EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        channel.queueBind(queueName, ClientWriter.EXCHANGE_NAME, "");
        channel.queueBind(queueName, ClientReader.EXCHANGE_NAME, ROUTING_KEY_READER);

        System.out.println(" [*] Waiting for messages on replica " + replicaNumber + ". To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [*] Received '" + message + "'");
            if (message.equals("Read Last")) {
                try {
                    String line = getLastLineOfFile(replicaDirectory + "/" + fileName) ;
                    getLinesOfFile(replicaDirectory + "/" + fileName,channel);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                addLineToFile(replicaDirectory + "/" + fileName, message);
            }
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }

    private static void processReadLast(Channel channel, String message) throws Exception {
//        channel.exchangeDeclare(ClientReader.EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        channel.basicPublish(ClientReader.EXCHANGE_NAME, ClientReader.ROUTING_KEY, null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [*] Sent '" + message + "'");
    }

    private static void addLineToFile(String filePath, String line) {
        try {
            File file = new File(filePath);
            if (!file.exists() && !file.createNewFile()) System.out.println("file creation error");

            FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(line + "\n");
            bw.close();
//            System.out.println(" [x] Added line to file: " + line);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getLastLineOfFile(String filePath) throws IOException {
        String lastLine = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lastLine = line;
            }
        }
        return lastLine;
    }

    private static void getLinesOfFile(String filePath,Channel channel) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {

                processReadLast(channel,line);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //return lastLine;
    }

}

