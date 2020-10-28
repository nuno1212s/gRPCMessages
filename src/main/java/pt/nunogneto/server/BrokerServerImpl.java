package pt.nunogneto.server;

import io.grpc.stub.StreamObserver;
import pt.nunogneto.*;
import pt.nunogneto.server.database.BrokerDatabase;
import pt.nunogneto.server.database.JSONBrokerDatabase;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BrokerServerImpl extends BrokerGrpc.BrokerImplBase {

    private final BrokerDatabase database;

    private final Logger logger;

    public BrokerServerImpl(Logger logger) {
        this.database = new JSONBrokerDatabase(TimeUnit.HOURS.toMillis(1));
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
    public StreamObserver<TagSubscription> subscribeToTags(StreamObserver<TagMessage> responseObserver) {
        return new StreamObserver<TagSubscription>() {
            @Override
            public void onNext(TagSubscription value) {
                subscribeToTag(value, responseObserver);
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                //Finished announcing the tags
            }
        };
    }

    @Override
    public void subscribeToTag(TagSubscription request, StreamObserver<TagMessage> responseObserver) {

        String tag = request.getTagName();

        logger.log(Level.INFO, "Client subscribed to {0}", tag);

        database.registerSubscriber(tag, responseObserver);
    }

    @Override
    public void registerAsProvider(ProviderRegistration request, StreamObserver<ProviderRegisterResult> responseObserver) {

        String tag = request.getTagProvider();

        database.registerPublisher(tag);

        ProviderRegisterResult registerResult = ProviderRegisterResult.newBuilder()
                .build();

        responseObserver.onNext(registerResult);

        responseObserver.onCompleted();

    }

    @Override
    public void publishMessageIndividual(MessageToPublish request, StreamObserver<PublishResult> responseObserver) {
        database.publishMessage(request);

        PublishResult build = PublishResult.newBuilder().setResult(1).build();

        responseObserver.onNext(build);

        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<MessageToPublish> publishMessage(StreamObserver<PublishResult> responseObserver) {

        StreamObserver<MessageToPublish> observer = new StreamObserver<MessageToPublish>() {

            private boolean registered = false;

            @Override
            public void onNext(MessageToPublish value) {
                String message = value.getMessage(), tag = value.getTag();

                if (!registered) {
                    database.registerPublisher(tag, this);
                }

                database.publishMessage(value);

                PublishResult result = PublishResult.newBuilder().setResult(1).build();

                responseObserver.onNext(result);

                logger.log(Level.INFO, "Received a message to publish in the tag: {0}. Message: {1}", new Object[]{tag, message});
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

                logger.log(Level.INFO, "Publisher disconnected.");

                responseObserver.onCompleted();
            }
        };

        return observer;
    }
}
