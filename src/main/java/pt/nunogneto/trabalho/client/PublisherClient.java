package pt.nunogneto.trabalho.client;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.cli.*;
import pt.nunogneto.trabalho.*;
import pt.nunogneto.trabalho.util.DataParser;
import pt.nunogneto.trabalho.util.FileDataParser;
import pt.nunogneto.trabalho.util.SomeSentencesParser;

import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PublisherClient extends Client {

    private static int TIME_FRAME = 3600;

    private static float AVERAGE_MESSAGES_DESIRED = 12;

    private static final Logger logger = Logger.getLogger(PublisherClient.class.getName());

    private static final Random random = new Random();

    private final AtomicBoolean active = new AtomicBoolean(true);

    private final BrokerGrpc.BrokerStub futureStub;

    private StreamObserver<MessageToPublish> publishStream;

    private static List<String> randomMessages;

    public PublisherClient(Channel channel, DataParser parser) {
        super(parser);

        randomMessages = parser.readPublisherSentences();

        this.futureStub = BrokerGrpc.newStub(channel);

        logger.log(Level.INFO, "Starting publisher with tag {0}", getTag());
    }

    public PublisherClient(Channel channel, DataParser parser, String tag) {
        super(parser, tag);

        randomMessages = parser.readPublisherSentences();

        this.futureStub = BrokerGrpc.newStub(channel);

        logger.log(Level.INFO, "Starting publisher with tag {0}", getTag());
    }

    private static List<String> getRandomMessages() {
        return randomMessages;
    }

    public String getNextMessage() {
        return getRandomMessages().get(random.nextInt(getRandomMessages().size()));
    }

    public static int getPoisson(double lambda) {

        double logResult = -(Math.log(1 - Math.random())), divided = logResult / lambda;

//        logger.log(Level.INFO, "The log value is {0} and divided is {1}",
//                new Object[]{logResult, divided});

        return (int) Math.ceil(divided);
    }

    private void publishReceivedMessage(Scanner scanner) {
        System.out.println("Type \"q\" to quit from publishing.");

        String messageReceived = null;

        do {

            if (messageReceived != null) {
                publishMessage(getTag(), messageReceived);
            }

            messageReceived = scanner.nextLine();

        } while (!messageReceived.equals("q"));
    }

    private void generateRandomMessages() {
        float averageTimeBetween = (TIME_FRAME / AVERAGE_MESSAGES_DESIRED);

//        logger.log(Level.INFO, "Time between {0}, lambda would be {1}", new Object[]{averageTimeBetween, 1 / averageTimeBetween});

        while (true) {

            if (!active.get()) {
                break;
            }

            publishMessage(getTag(), getNextMessage());

            int toSleep = getPoisson(1 / averageTimeBetween) * 1000;

            logger.log(Level.INFO, "Sleeping for {0} ms", toSleep);

            try {
                Thread.sleep(toSleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void generateMessages(boolean manual) {
        if (manual) {
            publishReceivedMessage(new Scanner(System.in));
        } else {
            generateRandomMessages();
        }

    }

    private void deactivate() {
        active.set(false);

        if (this.publishStream != null) {
            this.publishStream.onCompleted();
        }
    }

    private StreamObserver<MessageToPublish> initStream() {

        StreamObserver<KeepAlive> publishResultHandler = new StreamObserver<KeepAlive>() {
            @Override
            public void onNext(KeepAlive value) {
                logger.log(Level.SEVERE, "Received keep alive.");
            }

            @Override
            public void onError(Throwable t) {
                logger.log(Level.SEVERE, "An error has occurred.");
            }

            @Override
            public void onCompleted() {
                logger.log(Level.SEVERE, "The broker server has been disconnected.");
            }
        };

        return this.futureStub.publishMessage(publishResultHandler);
    }

    public void publishMessage(String tag, String message) {

        if (this.publishStream == null) {
            this.publishStream = initStream();
        }

        MessageToPublish builtMessage = MessageToPublish.newBuilder().setId(random.nextLong())
                .setDate(System.currentTimeMillis()).setTag(tag).setMessage(message).build();

        this.publishStream.onNext(builtMessage);

    }

    public void donePublishing() {
        this.publishStream.onCompleted();
    }

    public static void main(String[] args) {

        String target = Client.TARGET;

        Options options = new Options();

        Option connectionIp = new Option("ip", "target", true,
                "Choose the IP you want to connect to.");
        connectionIp.setRequired(false);
        options.addOption(connectionIp);

        Option publishTag = new Option("t", "tag", true,
                "Select the tag you want to publish to.");
        publishTag.setRequired(false);
        options.addOption(publishTag);

        Option manual = new Option("m", "manual", false,
                "Should the messages be read from the terminal or be generated automatically.");
        manual.setRequired(false);
        options.addOption(manual);

        Option inputFile = new Option("if", "inputfile", true,
                "Select the input file for random generated messages.");
        inputFile.setRequired(false);
        options.addOption(inputFile);

        Option messagesPerHour = new Option("mph", "messagesph", true,
                "Messages per hour for the random generator");
        messagesPerHour.setRequired(false);
        options.addOption(messagesPerHour);

        CommandLineParser cmdLineParser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = cmdLineParser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Publisher", options);

            System.exit(1);
            return;
        }

        DataParser parser = new SomeSentencesParser();

        boolean isManual = cmd.hasOption("m");

        if (cmd.hasOption("ip")) {
            target = cmd.getOptionValue("ip");
        }

        if (cmd.hasOption("if")) {
            parser = new FileDataParser(cmd.getOptionValue("if"));
        }

        String tag = null;

        if (cmd.hasOption("t")) {
            tag = cmd.getOptionValue("t");
        }

        if (cmd.hasOption("mph")) {
            AVERAGE_MESSAGES_DESIRED = Float.parseFloat(cmd.getOptionValue("mph"));
        }

        final ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();

        final PublisherClient client;

        if (tag != null) {
            client = new PublisherClient(channel, parser, tag);
        } else {
            client = new PublisherClient(channel, parser);
        }

        try {

            client.generateMessages(isManual);

            client.donePublishing();

        } finally {
            try {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(client::deactivate));
    }

}
