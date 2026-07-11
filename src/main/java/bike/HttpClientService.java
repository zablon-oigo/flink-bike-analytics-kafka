package bike;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class HttpClientService {

    private static final String BIKE_API =
            "https://tfl.gov.uk/tfl/syndication/feeds/cycle-hire/livecyclehireupdates.xml";

    private final HttpClient client;

    public HttpClientService() {

        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public String fetchBikeData() {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BIKE_API))
                .header("Accept", "application/xml")
                .GET()
                .build();

        try {

            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException(
                        "HTTP Error: " + response.statusCode());
            }

            return response.body();

        } catch (IOException | InterruptedException e) {

            Thread.currentThread().interrupt();
            throw new RuntimeException(e);

        }
    }
}