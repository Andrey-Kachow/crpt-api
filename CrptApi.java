import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * A CrptApi object that manages the rate-limited creation of documents via a REST API.
 * It limits the number of requests within a specified time window to prevent overload.
 * The requests that exceed the limit are queued and processed as the rate allows.
 * <p>
 * Usage example:
 * <pre>
 * {@code
 *  final CrptApi crptApi = new CrptApi(TimeUnit.SECONDS, 5);
 *
 *  // Simulate 25 requests
 *  for (int i = 0; i < 25; i++) {
 *      crptApi.createDocument(new ExampleDocumentProvider());
 *      // Simulate a small delay between requests
 *      Thread.sleep(50);  // Sleep for 50 milliseconds
 *  }
 *
 *  // Give some time for processing queued requests
 *  Thread.sleep(60000);
 *
 *  // Stop the rate limiter gracefully
 *  crptApi.stop();
 *  }
 * </pre>
 */
public class CrptApi {
    private static final int SCHEDULED_THREAD_POOL_SIZE = 1;
    private static final String DOCUMENT_CREATION_POST_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";

    private final long requestTimeLimitWindow;
    private final int requestLimit;
    private final Deque<Long> requestTimestamps = new LinkedList<>();
    private final Queue<Runnable> pendingRequests = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(SCHEDULED_THREAD_POOL_SIZE);

    public CrptApi(final TimeUnit timeUnit, final int requestLimit) {
        this.requestTimeLimitWindow = timeUnit.toMillis(1);
        this.requestLimit = requestLimit;
        startRequestProcessing();
    }

    private static long getNow() {
        return Instant.now().toEpochMilli();
    }

    private void sendRequest(final Runnable request) {
        synchronized (this) {
            removeOldRequestsOutsideTimeWindow(getNow());
            pendingRequests.add(request);
        }
    }

    private void removeOldRequestsOutsideTimeWindow(final long now) {
        while (!requestTimestamps.isEmpty() && now - requestTimestamps.peekFirst() > requestTimeLimitWindow) {
            requestTimestamps.pollFirst();
        }
    }


    private void startRequestProcessing() {
        scheduler.scheduleAtFixedRate(() -> {
            long now = getNow();
            synchronized (this) {
                removeOldRequestsOutsideTimeWindow(now);
                while (!pendingRequests.isEmpty() && requestTimestamps.size() < requestLimit) {
                    Runnable request = pendingRequests.poll();
                    requestTimestamps.addLast(getNow());
                    request.run();
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Create and submit new request job for the execution service
     *
     * @param provider is an object that gives the json for the request body
     */
    public void createDocument(final DocumentDataProvider provider) {
        Runnable sendPostRequest = () -> {
            try {
                HttpClient client = HttpClient.newHttpClient();

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(DOCUMENT_CREATION_POST_URL))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(provider.provideJson(), StandardCharsets.UTF_8))
                        .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                System.out.println("Response Code: " + response.statusCode());
                System.out.println("Response Body: " + response.body());
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        };
        sendRequest(sendPostRequest);
    }

    /**
     * Stop the scheduler from checking the pending requests queue
     */
    public void stop() {
        scheduler.shutdown();
    }

    /**
     * An interface for strategy of providing a json
     */
    interface DocumentDataProvider {
        String provideJson();
    }

    /**
     * Simple provider for the document data json. A more practical approach would involve a
     * serializable object that can store the document data according to a well-defined spec.
     */
    public static class ExampleDocumentProvider implements DocumentDataProvider {
        @Override
        public String provideJson() {
            return """
                        {
                            "description": {
                                "participantInn": "string"
                            },
                            "doc_id": "string",
                            "doc_status": "string",
                            "doc_type": "LP_INTRODUCE_GOODS",
                            109 "importRequest": true,
                            "owner_inn": "string",
                            "participant_inn": "string",
                            "producer_inn": "string",
                            "production_date": "2020-01-23",
                            "production_type": "string",
                            "products": [
                                {
                                    "certificate_document": "string",
                                    "certificate_document_date": "2020-01-23",
                                    "certificate_document_number": "string",
                                    "owner_inn": "string",
                                    "producer_inn": "string",
                                    "production_date": "2020-01-23",
                                    "tnved_code": "string",
                                    "uit_code": "string",
                                    "uitu_code": "string"
                                }
                            ],
                            "reg_date": "2020-01-23",
                            "reg_number": "string"
                        }
                    """.stripIndent();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        final CrptApi crptApi = new CrptApi(TimeUnit.SECONDS, 5);

        // Simulate 25 requests
        for (int i = 0; i < 25; i++) {
            crptApi.createDocument(new ExampleDocumentProvider());
            // Simulate a small delay between requests
            Thread.sleep(50);  // Sleep for 50 milliseconds
        }

        // Give some time for processing queued requests
        Thread.sleep(60000);

        // Stop the rate limiter gracefully
        crptApi.stop();
    }
}
