package pt.nunogneto.server.database;

import com.google.protobuf.Message;
import io.grpc.stub.StreamObserver;
import pt.nunogneto.MessageToPublish;
import pt.nunogneto.PublishResult;
import pt.nunogneto.TagMessage;

import java.util.List;

public interface BrokerDatabase {

    List<String> getTagList();

    void registerSubscriber(String tag, StreamObserver<TagMessage> stream);

    void registerPublisher(String tag);

    void registerPublisher(String tag, StreamObserver<MessageToPublish> stream);

    void removePublisher(StreamObserver<MessageToPublish> publisher);

    void publishMessage(MessageToPublish toPublish);

}
