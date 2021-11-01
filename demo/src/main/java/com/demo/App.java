package com.demo;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.util.concurrent.Uninterruptibles;
import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.*;
import com.oracle.bmc.streaming.model.Stream.LifecycleState;
import com.oracle.bmc.streaming.requests.*;
import com.oracle.bmc.streaming.responses.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
        //final String configurationFilePath = "~/.oci/config";
        //final String profile = "DEFAULT";

        // Configuring the AuthenticationDetailsProvider. It's assuming there is a default OCI config file
        // "~/.oci/config", and a profile in that config with the name "DEFAULT". Make changes to the following
        // line if needed and use ConfigFileReader.parse(CONFIG_LOCATION, CONFIG_PROFILE);

        final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parseDefault();

        final AuthenticationDetailsProvider provider =
                new ConfigFileAuthenticationDetailsProvider(configFile);

        // Create an admin-client for the phoenix region.
        final StreamAdminClient adminClient = StreamAdminClient.builder().build(provider);

        if (args.length < 1) {
            throw new IllegalArgumentException(
                    "This example expects an ocid for the compartment in which streams should be created.");
        }

        final String compartmentId = args[0];
        final String exampleStreamName = "stream-sdk-test";
        final int partitions = 1;

        // We want to be good samaritan, so we'll reuse a stream if its already created.
        // This will utilize ListStreams() to determine if a stream exists and return it, or create a new one.
        Stream stream =
                getOrCreateStream(adminClient, compartmentId, exampleStreamName, partitions);

        // Streams are assigned a specific endpoint url based on where they are provisioned.
        // Create a stream client using the provided message endpoint.
        StreamClient streamClient = StreamClient.builder().stream(stream).build(provider);

        String streamId = stream.getId();

        // publish some messages to the stream
        publishExampleMessages(streamClient, streamId);

         // give the streaming service a second to propagate messages
         Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        // A cursor can be created as part of a consumer group.
        // Committed offsets are managed for the group, and partitions
        // are dynamically balanced amongst consumers in the group.
        System.out.println("Starting a simple message loop with a group cursor");
        String groupCursor =
                getCursorByGroup(streamClient, streamId, "exampleGroup", "exampleInstance-1");
        simpleMessageLoop(streamClient, streamId, groupCursor);

        // Cleanup; remember to delete streams which are not in use.
        deleteStream(adminClient, streamId);

        // Stream deletion is an asynchronous operation, give it some time to complete.

        GetStreamRequest streamRequest = GetStreamRequest.builder().streamId(streamId).build();
        adminClient.getWaiters().forStream(streamRequest, LifecycleState.Deleted).execute();

        System.out.println( "Hello World for TEST." );
    }

    private static Stream getOrCreateStream(
            StreamAdminClient adminClient, String compartmentId, String streamName, int partitions)
            throws Exception {

        ListStreamsRequest listRequest =
                ListStreamsRequest.builder()
                        .compartmentId(compartmentId)
                        .lifecycleState(LifecycleState.Active)
                        .name(streamName)
                        .build();

        ListStreamsResponse listResponse = adminClient.listStreams(listRequest);

        if (!listResponse.getItems().isEmpty()) {
            // if we find an active stream with the correct name, we'll use it.
            System.out.println(String.format("An active stream named %s was found.", streamName));

            String streamId = listResponse.getItems().get(0).getId();
            return getStream(adminClient, streamId);
        }

        System.out.println(
                String.format("No active stream named %s was found; creating it now.", streamName));
        Stream createdStream = createStream(adminClient, compartmentId, streamName, partitions);

        // GetStream provides details about a specific stream.
        // Since stream creation is asynchronous; we need to wait for the stream to become active.
        GetStreamRequest streamRequest =
                GetStreamRequest.builder().streamId(createdStream.getId()).build();
        Stream activeStream =
                adminClient
                        .getWaiters()
                        .forStream(streamRequest, LifecycleState.Active)
                        .execute()
                        .getStream();

        // Give a little time for the stream to be ready.
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        return activeStream;
    }

    private static void publishExampleMessages(StreamClient streamClient, String streamId) {
        // build up a putRequest and publish some messages to the stream
        List<PutMessagesDetailsEntry> messages = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            messages.add(
                    PutMessagesDetailsEntry.builder()
                            .key(String.format("messageKey-%s", i).getBytes(UTF_8))
                            .value(String.format("messageValue-%s", i).getBytes(UTF_8))
                            .build());
        }

        System.out.println(
                String.format("Publishing %s messages to stream %s.", messages.size(), streamId));
        PutMessagesDetails messagesDetails =
                PutMessagesDetails.builder().messages(messages).build();

        PutMessagesRequest putRequest =
                PutMessagesRequest.builder()
                        .streamId(streamId)
                        .putMessagesDetails(messagesDetails)
                        .build();

        PutMessagesResponse putResponse = streamClient.putMessages(putRequest);

        // the putResponse can contain some useful metadata for handling failures
        for (PutMessagesResultEntry entry : putResponse.getPutMessagesResult().getEntries()) {
            if (StringUtils.isNotBlank(entry.getError())) {
                System.out.println(
                        String.format("Error(%s): %s", entry.getError(), entry.getErrorMessage()));
            } else {
                System.out.println(
                        String.format(
                                "Published message to partition %s, offset %s.",
                                entry.getPartition(),
                                entry.getOffset()));
            }
        }
    }

    private static Stream getStream(StreamAdminClient adminClient, String streamId) {
        GetStreamResponse getResponse =
                adminClient.getStream(GetStreamRequest.builder().streamId(streamId).build());
        return getResponse.getStream();
    }

    private static Stream createStream(
        StreamAdminClient adminClient,
        String compartmentId,
        String streamName,
        int partitions) {
    System.out.println(
            String.format("Creating stream %s with %s partitions", streamName, partitions));

    CreateStreamDetails streamDetails =
            CreateStreamDetails.builder()
                    .compartmentId(compartmentId)
                    .name(streamName)
                    .partitions(partitions)
                    .build();

    CreateStreamRequest createStreamRequest =
            CreateStreamRequest.builder().createStreamDetails(streamDetails).build();

    CreateStreamResponse createResponse = adminClient.createStream(createStreamRequest);
    return createResponse.getStream();
   }

   private static String getCursorByGroup(
    StreamClient streamClient, String streamId, String groupName, String instanceName) {
System.out.println(
        String.format(
                "Creating a cursor for group %s, instance %s.", groupName, instanceName));

CreateGroupCursorDetails cursorDetails =
        CreateGroupCursorDetails.builder()
                .groupName(groupName)
                .instanceName(instanceName)
                .type(CreateGroupCursorDetails.Type.TrimHorizon)
                .commitOnGet(true)
                .build();

CreateGroupCursorRequest createCursorRequest =
        CreateGroupCursorRequest.builder()
                .streamId(streamId)
                .createGroupCursorDetails(cursorDetails)
                .build();

CreateGroupCursorResponse groupCursorResponse =
        streamClient.createGroupCursor(createCursorRequest);
return groupCursorResponse.getCursor().getValue();
}

private static void simpleMessageLoop(
    StreamClient streamClient, String streamId, String initialCursor) {
String cursor = initialCursor;
for (int i = 0; i < 10; i++) {

    GetMessagesRequest getRequest =
            GetMessagesRequest.builder()
                    .streamId(streamId)
                    .cursor(cursor)
                    .limit(10)
                    .build();

    GetMessagesResponse getResponse = streamClient.getMessages(getRequest);

    // process the messages
    System.out.println(String.format("Read %s messages.", getResponse.getItems().size()));
    for (Message message : getResponse.getItems()) {
        System.out.println(
                String.format(
                        "%s: %s",
                        new String(message.getKey(), UTF_8),
                        new String(message.getValue(), UTF_8)));
    }

    // getMessages is a throttled method; clients should retrieve sufficiently large message
    // batches, as to avoid too many http requests.
    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

    // use the next-cursor for iteration
    cursor = getResponse.getOpcNextCursor();
}
}

private static void deleteStream(StreamAdminClient adminClient, String streamId) {
    System.out.println("Deleting stream " + streamId);
    adminClient.deleteStream(DeleteStreamRequest.builder().streamId(streamId).build());
}

}
