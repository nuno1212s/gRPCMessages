package pt.nunogneto.trabalho.client;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import pt.nunogneto.trabalho.BrokerGrpc;
import pt.nunogneto.trabalho.KeepAlive;
import pt.nunogneto.trabalho.MessageToPublish;
import pt.nunogneto.trabalho.util.DataParser;
import pt.nunogneto.trabalho.util.FileDataParser;
import pt.nunogneto.trabalho.util.SomeSentencesParser;

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

    public PublisherClient(Channel channel, DataParser parser) {
        super(parser);

        this.futureStub = BrokerGrpc.newStub(channel);

        logger.log(Level.INFO, "Starting publisher with tag {0}", getTag());
    }

    public static int getPoisson(double lambda) {

        double logResult = -(Math.log(1 - Math.random())), divided = logResult / lambda;

//        logger.log(Level.INFO, "The log value is {0} and divided is {1}",
//                new Object[]{logResult, divided});

        return (int) Math.ceil(divided);
    }

    private void publishReceivedMessage(Scanner scanner) {

        System.out.println("Input the tag you want to publish to.");
        String tag = scanner.nextLine();

        String messageReceived = null;

        do {

            if (messageReceived != null) {
                publishMessage(tag, messageReceived);
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

    private void generateMessages() {

        Scanner scanner = new Scanner(System.in);

        System.out.println("Choose the input type:");
        System.out.println("1) Input from terminal");
        System.out.println("2) Generate random messages.");

        int i = scanner.nextInt();

        if (i == 1) {
            publishReceivedMessage(scanner);
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

        DataParser parser = new SomeSentencesParser();

        if (args.length >= 1) {
            target = args[0];
        }

        if (args.length >= 2) {
            if (args[1].equals("FILE")) {
                parser = new FileDataParser();
            }
        }

        final ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();

        final PublisherClient client = new PublisherClient(channel, parser);

        try {

            client.generateMessages();

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
