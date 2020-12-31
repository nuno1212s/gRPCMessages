package pt.nunogneto.trabalho.client;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.cli.*;
import pt.nunogneto.trabalho.BrokerGrpc;
import pt.nunogneto.trabalho.TagMessage;
import pt.nunogneto.trabalho.TagSubscription;
import pt.nunogneto.trabalho.util.DataParser;
import pt.nunogneto.trabalho.util.FileDataParser;
import pt.nunogneto.trabalho.util.SomeSentencesParser;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SubscriberClient extends Client {

    private static final Logger logger = Logger.getLogger(SubscriberClient.class.getName());

    private final BrokerGrpc.BrokerBlockingStub futureStub;

    public SubscriberClient(Channel channel, DataParser parser) {
        super(parser);

        this.futureStub = BrokerGrpc.newBlockingStub(channel);

        subscribeToTag(getTag());
    }

    public SubscriberClient(Channel channel, DataParser parser, String tag) {
        super(parser, tag);

        this.futureStub = BrokerGrpc.newBlockingStub(channel);

        subscribeToTag(tag);
    }

    public void subscribeToTag(String tag) {

        logger.log(Level.INFO, "Subscribed to tag {0}", tag);

        TagSubscription subscription = TagSubscription.newBuilder().setTagName(tag).build();

        final Iterator<TagMessage> tagMessageIterator = this.futureStub.subscribeToTag(subscription);

        while (tagMessageIterator.hasNext()) {

            final TagMessage next = tagMessageIterator.next();

            if (next.getIsKeepAlive()) {
                logger.log(Level.INFO, "Received keepalive message.");

                continue;
            }

            logger.log(Level.INFO, "Received a message for the tag {0}: {1}", new Object[]{tag, next.getMessage()});
        }

        logger.log(Level.INFO, "The server seems to have disconnected!");
    }

    public static void main(String[] args) {

        String target = Client.TARGET;

        DataParser parser = new SomeSentencesParser();

        Options options = new Options();

        Option connectionIp = new Option("ip", "target", true,
                "Choose the IP you want to connect to.");
        connectionIp.setRequired(false);
        options.addOption(connectionIp);

        Option publishTag = new Option("t", "tag", true,
                "Select the tag you want to subscribe to.");
        publishTag.setRequired(false);
        options.addOption(publishTag);

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

        if (cmd.hasOption("ip")) {
            target = cmd.getOptionValue("ip");
        }

        String tag = null;

        if (cmd.hasOption("t")) {
            tag = cmd.getOptionValue("t");
        }

        final ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        try {
            final SubscriberClient subscriberClient;

            if (tag == null) {
                subscriberClient = new SubscriberClient(channel, parser);
            } else {
                subscriberClient = new SubscriberClient(channel, parser, tag);
            }

        } finally {
            try {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
