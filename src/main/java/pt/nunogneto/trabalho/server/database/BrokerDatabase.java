package pt.nunogneto.trabalho.server.database;

import io.grpc.stub.StreamObserver;
import pt.nunogneto.trabalho.MessageToPublish;
import pt.nunogneto.trabalho.TagMessage;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface BrokerDatabase {

    List<String> getTagList();

    void registerSubscriber(String tag, StreamObserver<TagMessage> stream);

    void registerPublisher(String tag);

    void registerPublisher(String tag, StreamObserver<MessageToPublish> stream);

    void removePublisher(StreamObserver<MessageToPublish> publisher);

    void publishMessage(MessageToPublish toPublish);

    Map<String, Collection<StreamObserver<MessageToPublish>>> getActivePublishers();

    Map<String, Collection<StreamObserver<TagMessage>>> getActiveSubscribers();

    void shutdown();

}
