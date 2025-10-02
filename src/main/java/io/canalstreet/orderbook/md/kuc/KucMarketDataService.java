package io.canalstreet.orderbook.md.kuc;

import com.fasterxml.jackson.databind.JsonNode;
import io.canalstreet.orderbook.data.Instrument;
import io.canalstreet.orderbook.md.QuoteSide;
import io.canalstreet.orderbook.md.QuotesL2UpdateCache;
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
    private Map<Instrument, QuotesL2UpdateCache> quotesCache = HashMap.newHashMap(5);

    public void subscribe(Instrument instrument) {
        String token = restAdapter.fetchToken(instrument);
        if (token == null) {
            LOGGER.error("Failed to fetch the token. Aborting...");
            return;
        }
        webSocketAdapter.subscribe(instrument, token, this::handleMessage, () -> subscribe(instrument));
        this.quotesL2Books.put(instrument, new QuotesL2Book(instrument, 10, 1000));
        this.quotesCache.put(instrument, new QuotesL2UpdateCache());
    }

    public void unsubscribe(Instrument instrument) {
        webSocketAdapter.unsubscribe(instrument);
    }

    public QuotesL2Book getQuotesL2(Instrument instrument) {
        return quotesL2Books.get(instrument);
    }

    private void handleMessage(Instrument instrument, JsonNode message) {
        QuotesL2UpdateCache cache = quotesCache.get(instrument);
        QuotesL2Book book = quotesL2Books.get(instrument);
        if ("trade.l2update".equals(message.get("subject").asText())) {
            long sequenceStart = message.at("/dara/sequenceStart").asLong();
            long sequenceEnd = message.at("/dara/sequenceStart").asLong();
            for (JsonNode bids : message.at("/data/changes/bids")) {
                long price = instrument.priceToInt(bids.get(0).asDouble());
                int quantity = instrument.qtyToInt(bids.get(1).asDouble());
                cache.add(BID, price, quantity, sequenceStart, sequenceEnd);
            }
            for (JsonNode asks : message.at("/data/changes/asks")) {
                long price = instrument.priceToInt(asks.get(0).asDouble());
                int quantity = instrument.qtyToInt(asks.get(1).asDouble());
                cache.add(ASK, price, quantity, sequenceStart, sequenceEnd);
            }
            if (quotesL2Books.get(instrument).isInitialized()) {
                cache.move(quotesL2Books.get(instrument));
            }

            // Initial population of the book. Check for code=2000 could be improved/changed.
            if ("2000".equals(message.get("code").asText())) {
                for (JsonNode bids : message.at("/data/bids")) {
                    long price = instrument.priceToInt(bids.get(0).asDouble());
                    int quantity = instrument.qtyToInt(bids.get(1).asDouble());
                    book.add(BID, price, quantity, sequenceStart, sequenceEnd);
                }
                for (JsonNode asks : message.at("/data/asks")) {
                    long price = instrument.priceToInt(asks.get(0).asDouble());
                    int quantity = instrument.qtyToInt(asks.get(1).asDouble());
                    book.add(ASK, price, quantity, sequenceStart, sequenceEnd);
                }
            }
        }
    }

}
