package io.canalstreet.orderbook.md;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.LongArrayDeque;
import lombok.Data;

import static io.canalstreet.orderbook.md.QuoteSide.BID;

@Data
public class QuotesL2UpdateCache {

    private LongArrayDeque sequenceStartQueue = new LongArrayDeque(1000);
    private LongArrayDeque sequenceEndQueue = new LongArrayDeque(1000);
    private LongArrayDeque bidsPriceQueue = new LongArrayDeque(1000);
    private IntArrayDeque bidsQuantityQueue = new IntArrayDeque(1000);
    private LongArrayDeque asksPriceQueue = new LongArrayDeque(1000);
    private IntArrayDeque asksQuantityQueue = new IntArrayDeque(1000);

    public boolean isEmpty() {
        return sequenceStartQueue.isEmpty() && sequenceEndQueue.isEmpty();
    }

    public void add(QuoteSide side, long price, int quantity, long sequenceStart, long sequenceEnd) {
        sequenceStartQueue.addFirst(sequenceStart);
        sequenceEndQueue.addFirst(sequenceEnd);
        if (side == BID) {
            bidsPriceQueue.addFirst(price);
            bidsQuantityQueue.addFirst(quantity);
        }
        if (side == QuoteSide.ASK) {
            asksPriceQueue.addFirst(price);
            asksQuantityQueue.addFirst(quantity);
        }
    }

    public void move(QuotesL2Book book) {
        while(!isEmpty()) {
            long sequenceStart = sequenceStartQueue.removeFirst();
            long sequenceEnd = sequenceEndQueue.removeFirst();

            long bidPrice = bidsPriceQueue.removeFirst();
            int bidQuantity = bidsQuantityQueue.removeFirst();
            if (sequenceStart <= book.getLastSequenceEnd()+1 && sequenceEnd > book.getLastSequenceEnd()) {
                book.add(BID, bidPrice, bidQuantity);
            }
            long askPrice = bidsPriceQueue.removeFirst();
            int askQuantity = bidsQuantityQueue.removeFirst();
            if (sequenceStart <= book.getLastSequenceEnd()+1 && sequenceEnd > book.getLastSequenceEnd()) {
                book.add(BID, askPrice, askQuantity);
            }
        }
    }
}
