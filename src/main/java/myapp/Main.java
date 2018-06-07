package myapp;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.joda.time.DateTime;

public final class Main {

    private static final String PROJECT_ID = "pubsub-in-gae-te-1528383772593";
    private static final String TOPIC_ID = "test_topic_mj";


    public static void main(String[] args) {
        try {
            publishMessage(DateTime.now().toDateTime().toString());
        } catch (Exception ex) {
            System.out.println("Something went wrong when publishing message!" + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private static void publishMessage(String message) throws Exception {
        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, TOPIC_ID);
        Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(topicName).build();
            ByteString data = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                    .setData(data)
                    .build();

            //schedule a message to be published, messages are automatically batched
            //only publish, don't care for the outcome
            publisher.publish(pubsubMessage);
        } finally {
            if (publisher != null) {
                publisher.shutdown();
            }
        }
    }
}