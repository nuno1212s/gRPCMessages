package pt.nunogneto.trabalho.server;

import io.grpc.stub.StreamObserver;
import pt.nunogneto.trabalho.*;
import pt.nunogneto.trabalho.server.database.BrokerDatabase;
import pt.nunogneto.trabalho.server.database.JSONBrokerDatabase;
import pt.nunogneto.trabalho.util.SomeSentencesParser;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BrokerServerImpl extends BrokerGrpc.BrokerImplBase {

    private final BrokerDatabase database;

    private final Logger logger;

    public BrokerServerImpl(Logger logger, long messageExpirationTime) {
        this.database = new JSONBrokerDatabase(messageExpirationTime, new SomeSentencesParser());
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

                //perform a copy because I was getting a weird error with unallocated messages???
                //Maybe creating a new one will fix it.
                MessageToPublish messageCopy = MessageToPublish.newBuilder()
                        .setMessage(value.getMessage())
                        .setTag(value.getTag())
                        .setId(value.getId())
                        .setDate(value.getDate()).build();

                database.publishMessage(messageCopy);

                logger.log(Level.INFO, "Received a message to publish in the tag: {0}. Message: {1}", new Object[]{tag, message});
            }

            @Override
            public void onError(Throwable t) {
                logger.log(Level.WARNING, "Publisher client has disconnected.");

                database.removePublisher(responseObserver);
            }

            @Override
            public void onCompleted() {

                logger.log(Level.INFO, "Publisher disconnected.");

                try {
                    database.removePublisher(responseObserver);

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
