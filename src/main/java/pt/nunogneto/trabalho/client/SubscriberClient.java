package pt.nunogneto.trabalho.client;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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

    public void subscribeToTag(String tag) {

        logger.log(Level.INFO, "Subscribed to tag {0}", tag);

        TagSubscription subscription = TagSubscription.newBuilder().setTagName(tag).build();

        final Iterator<TagMessage> tagMessageIterator = this.futureStub.subscribeToTag(subscription);

        while (tagMessageIterator.hasNext()) {

            final TagMessage next = tagMessageIterator.next();

            logger.log(Level.INFO, "Received a message for the tag {0}: {1}", new Object[]{tag, next.getMessage()});
        }
    }

    public static void main(String[] args) {

        String target = "localhost:50051";

        DataParser parser = new SomeSentencesParser();

        if (args.length >= 1) {
            target = args[0];
        }

        if (args.length >= 2) {
            if (args[1].equals("FILE")) {
                parser = new FileDataParser();
            }
        }

        final ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        try {
            final SubscriberClient subscriberClient = new SubscriberClient(channel, parser);
        } finally {
            try {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
