package io.canalstreet.orderbook.md.kuc;

import com.fasterxml.jackson.databind.JsonNode;
import io.canalstreet.orderbook.data.Instrument;
import io.canalstreet.orderbook.md.QuotesL2Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

import static io.canalstreet.orderbook.md.QuoteSide.ASK;
import static io.canalstreet.orderbook.md.QuoteSide.BID;

@Service
public class KucMarketDataService {
    private static  final Logger LOGGER = LoggerFactory.getLogger(KucMarketDataService.class);

    private final KucRestAdapter restAdapter = new KucRestAdapter();
    private final KucWebSocketAdapter webSocketAdapter = new KucWebSocketAdapter();
    private Map<Instrument, QuotesL2Book> quotesL2Books = HashMap.newHashMap(5);

    public void subscribe(Instrument instrument) {
        String token = restAdapter.fetchToken(instrument);
        if (token == null) {
            LOGGER.error("Failed to fetch the token. Aborting...");
            return;
        }
        webSocketAdapter.subscribe(instrument, token, this::handleMessage, () -> subscribe(instrument));
        this.quotesL2Books.put(instrument, new QuotesL2Book(instrument, 10, 1000));
    }

    public void unsubscribe(Instrument instrument) {
        webSocketAdapter.unsubscribe(instrument);
    }

    public QuotesL2Book getQuotesL2(Instrument instrument) {
        return quotesL2Books.get(instrument);
    }

    private void handleMessage(Instrument instrument, JsonNode message) {
        if ("level2".equals(message.get("subject").asText())) {
            for (JsonNode bids : message.at("/data/bids")) {
                long price = instrument.priceToInt(bids.get(0).asDouble());
                int quantity = instrument.qtyToInt(bids.get(1).asDouble());
                quotesL2Books.get(instrument).add(BID, price, quantity);
            }
            for (JsonNode asks : message.at("/data/asks")) {
                long price = instrument.priceToInt(asks.get(0).asDouble());
                int quantity = instrument.qtyToInt(asks.get(1).asDouble());
                quotesL2Books.get(instrument).add(ASK, price, quantity);
            }
        }
    }
}
