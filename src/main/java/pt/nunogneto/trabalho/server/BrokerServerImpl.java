package pt.nunogneto.trabalho.server;

import io.grpc.stub.StreamObserver;
import pt.nunogneto.trabalho.*;
import pt.nunogneto.trabalho.server.database.BrokerDatabase;
import pt.nunogneto.trabalho.server.database.JSONBrokerDatabase;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BrokerServerImpl extends BrokerGrpc.BrokerImplBase {

    private final BrokerDatabase database;

    private final Logger logger;

    public BrokerServerImpl(Logger logger, long messageExpirationTime) {
        this.database = new JSONBrokerDatabase(messageExpirationTime);
        this.logger = logger;

        logger.log(Level.INFO, "Starting broker server....");

        logger.log(Level.INFO, "Waiting for connections.");
    }

    @Override
    public void getTagList(TagRequest request, StreamObserver<Tag> responseObserver) {

        List<String> tagList = database.getTagList();

        logger.log(Level.INFO, "Requested tag list. Provided tags: " + tagList);

        for (String tagName : tagList) {
            Tag tagMessage = Tag.newBuilder().setTagName(tagName).build();

            responseObserver.onNext(tagMessage);
        }

        responseObserver.onCompleted();

    }

    @Override
    public void subscribeToTag(TagSubscription request, StreamObserver<TagMessage> responseObserver) {

        String tag = request.getTagName();

        logger.log(Level.INFO, "Client subscribed to {0}", tag);

        database.registerSubscriber(tag, responseObserver);
    }

    @Override
    public StreamObserver<MessageToPublish> publishMessage(StreamObserver<KeepAlive> responseObserver) {

        return new StreamObserver<MessageToPublish>() {

            private boolean registered = false;

            @Override
            public void onNext(MessageToPublish value) {
                String message = value.getMessage(), tag = value.getTag();

                if (!registered) {
                    database.registerPublisher(tag, responseObserver);
                }

                database.publishMessage(value);

                logger.log(Level.INFO, "Received a message to publish in the tag: {0}. Message: {1}", new Object[]{tag, message});
            }

            @Override
            public void onError(Throwable t) {
                logger.log(Level.WARNING, "Publisher client has disconnected. {0}", t.getMessage());

                database.removePublisher(responseObserver);
            }

            @Override
            public void onCompleted() {

                logger.log(Level.INFO, "Publisher disconnected.");

                try {
                    responseObserver.onCompleted();
                } catch (Exception ignored) {

                }
            }
        };
    }

    public void shutdown() {
        this.database.shutdown();
    }
}
