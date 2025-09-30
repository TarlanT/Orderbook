package io.canalstreet.orderbook.md;

import io.canalstreet.orderbook.data.Instrument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static io.canalstreet.orderbook.md.QuoteSide.ASK;
import static io.canalstreet.orderbook.md.QuoteSide.BID;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class QuotesL2BookTest {

    Instrument instrument = Instrument.builder().symbol("BTCUSD").currency("USD").quantityScale(2).priceScale(2).tickSize(0.01f).pegIndexPrice(100_000).build();
    long pegIndexPrice = instrument.priceToInt(100_000);
    int l2depth = 20;
    int bookSize = 100;
    QuotesL2Book book = new QuotesL2Book(instrument, l2depth, bookSize);
    long initialBestBid;
    long initialBestAsk;

    @BeforeEach
    public void setup() {
        // Populate the book
        for (int i = 1; i <= l2depth; i++) {
            long priceBid = pegIndexPrice - i * instrument.tickSizeInt();
            long priceAsk = pegIndexPrice + i * instrument.tickSizeInt();
            book.add(BID, priceBid, instrument.qtyToInt(100+i));
            book.add(ASK, priceAsk, instrument.qtyToInt(100+i));
        }
        initialBestBid = book.getBestBid();
        initialBestAsk = book.getBestAsk();
    }

    @CsvSource({ // Number of ticks of price rise rotating QuotesL2Book circular buffer
            "20", // No rotation
            "40", // Partial rotation
            "60", // Full rotation
           "200", // Double rotation
    })
    @ParameterizedTest
    public void testPriceRise(int ticksUp) {
        for (int i = 1; i <= ticksUp; i++) {
            long newBestAsk = book.getBestAsk() + instrument.tickSizeInt();
            long newBestBid = book.getBestBid() + instrument.tickSizeInt();
            long newBoundaryPx = book.getBestAsk() + l2depth * instrument.tickSizeInt();
            book.add(ASK, book.getBestAsk(), 0);
            book.add(ASK, newBestAsk, instrument.qtyToInt(i));
            book.add(ASK, newBoundaryPx, instrument.qtyToInt(i));
            book.add(BID, newBestBid, instrument.qtyToInt(i));
        }
        assertEquals(initialBestBid + ticksUp * instrument.tickSizeInt(), book.getBestBid());
        assertEquals(initialBestAsk + ticksUp * instrument.tickSizeInt(), book.getBestAsk());
        assertEquals(((bookSize/2) + ticksUp - 1) % bookSize, book.toIndex(book.getBestBid()));
        assertEquals(((bookSize/2) + ticksUp + 1) % bookSize, book.toIndex(book.getBestAsk()));
    }

    @CsvSource({ // Number of ticks of price fall rotating QuotesL2Book circular buffer
            "20", // No rotation
            "40", // Partial rotation
            "60", // Full rotation
            "200", // Double rotation
    })
    @ParameterizedTest
    public void testPriceFall(int ticksDown) {
        for (int i = 1; i <= ticksDown; i++) {
            long newBestAsk = book.getBestAsk() - instrument.tickSizeInt();
            long newBestBid = book.getBestBid() - instrument.tickSizeInt();
            long newBoundaryPx = book.getBestBid() - l2depth * instrument.tickSizeInt();
            book.add(BID, book.getBestBid(), 0);
            book.add(BID, newBestBid, instrument.qtyToInt(i));
            book.add(BID, newBoundaryPx, instrument.qtyToInt(i));
            book.add(ASK, newBestAsk, instrument.qtyToInt(i));
        }
        assertEquals(initialBestBid - ticksDown * instrument.tickSizeInt(), book.getBestBid());
        assertEquals(initialBestAsk - ticksDown * instrument.tickSizeInt(), book.getBestAsk());
        int rawIndexBid = (bookSize/2) - ticksDown - 1;
        int rawIndexAsk = (bookSize/2) - ticksDown + 1;
        assertEquals(rawIndexBid > 0 ? rawIndexBid : bookSize + rawIndexBid % bookSize, book.toIndex(book.getBestBid()));
        assertEquals(rawIndexAsk > 0 ? rawIndexAsk : bookSize + rawIndexAsk % bookSize, book.toIndex(book.getBestAsk()));
    }
}
