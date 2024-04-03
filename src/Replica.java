import com.rabbitmq.client.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Replica {
    private final static String EXCHANGE_NAME = "file_text_exchange";
    private final static String EXCHANGE_READ = "read_exchange";
    private final static String QUEUE_NAME_PREFIX = "file_text_queue_";

    private static String replicaDirectory; // Répertoire du processus Replica
    private static String fileName = "replica_file.txt"; // Nom du fichier local

    public static void main(String[] argv) throws Exception {
        if (argv.length != 1) {
            System.out.println("Usage: Java Replica <replica_number>");
            System.exit(1);
        }

        int replicaNumber = Integer.parseInt(argv[0]);
        String queueName = QUEUE_NAME_PREFIX + replicaNumber;

        replicaDirectory = "replica_" + replicaNumber; // Créer un répertoire unique pour chaque processus Replica

        // Vérifier et créer le répertoire si nécessaire
        File directory = new File(replicaDirectory);
        if (!directory.exists()) {
            directory.mkdir();
        }

//        processReadLast();

        // Établir une connexion à RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost"); // Adresse du broker RabbitMQ
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // Déclarer la file d'attente correspondante
        channel.queueDeclare(queueName, false, false, false, null);

        // Déclarer l'échange de type fanout
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

        // Lier la file d'attente à l'échange
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        channel.exchangeDeclare(EXCHANGE_READ, BuiltinExchangeType.FANOUT);
        channel.queueBind(queueName, EXCHANGE_READ, "");

        System.out.println(" [*] Waiting for messages on replica " + replicaNumber + ". To exit press CTRL+C");

        // Créer un consommateur pour recevoir les messages
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            if (message.equals("Read Last")) {
                try {
                    processReadLast();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            addLineToFile(message);
        };

        // Consommer les messages de la file d'attente
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });

    }

    private static void addLineToFile(String line) {
        try {
            // Créer le chemin complet du fichier dans le répertoire du Replica
            String filePath = replicaDirectory + "/" + fileName;
            File file = new File(filePath);
            file.createNewFile();


            // Écrire la ligne dans le fichier
            FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(line + "\n");
            bw.close();

            System.out.println(" [x] Added line to file: " + line);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processReadLast() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        String message = "selem";

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare("direct_read", BuiltinExchangeType.DIRECT);
            channel.basicPublish("direct_read", "reader_queue", null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + "reader_queue" + "':'" + message + "'");

        }
        System.out.println("Jeni read");
    }
}
