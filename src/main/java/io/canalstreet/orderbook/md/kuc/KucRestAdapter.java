package io.canalstreet.orderbook.md.kuc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.canalstreet.orderbook.data.Instrument;
import io.canalstreet.orderbook.data.PerpetualFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class KucRestAdapter {
    private static  final Logger LOGGER = LoggerFactory.getLogger(KucRestAdapter.class);
    private static final int MAX_ATTEMPTS = 10;
    private static final ObjectMapper OM = new ObjectMapper();

    private HttpClient httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();

    public String fetchToken(Instrument instrument) {
        String url = instrument instanceof PerpetualFuture ? "https://api-futures.kucoin.com/api/v1/bullet-public" : "https://api.kucoin.com/api/v1/bullet-public";

        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.noBody())
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(10))
                .build();

        int attemtp = 1;
        while (attemtp < MAX_ATTEMPTS) {
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    String token = OM.readTree(response.body()).at("/data/token").asText();
                    LOGGER.info("Successfully fetched public token: {}", token);
                    return token;
                } else {
                    LOGGER.error("Failed to fetch token for {}. Status code: {}. Headers: {} Body: {}", instrument.getSymbol(), response.statusCode(), response.headers(), response.body());
                    return null;
                }
            } catch (Exception ex) {
                LOGGER.error("Failed to fetch L2 book for {}", instrument.getSymbol());
                attemtp++;
                if (attemtp < MAX_ATTEMPTS) {
                    LOGGER.error("Attempt: {} Retrying...", attemtp, ex);
                } else {
                    LOGGER.error("Attempt: {} Reached MAX_ATTEMPTS of {}. Aborting...", attemtp, MAX_ATTEMPTS, ex);
                    break;
                }
            }
        }
        return null;
    }

}
