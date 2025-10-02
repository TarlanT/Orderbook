package io.canalstreet.orderbook.md;

import com.carrotsearch.hppc.IntArrayList;
import io.canalstreet.orderbook.data.Instrument;

import java.util.ArrayList;
import java.util.List;

import static io.canalstreet.orderbook.md.QuoteSide.ASK;
import static io.canalstreet.orderbook.md.QuoteSide.BID;

/**
 * Ring buffer enabled high performance Zero GC implementation of Order Book.
 */
public class QuotesL2Book  {

    private final Instrument instrument;
    private final int l2depth;
    private final IntArrayList quoteQtys;
    private final int pegIndex;
    private final long pegIndexPrice;
    private long bestBidPrice = Integer.MIN_VALUE;
    private long bestAskPrice = Integer.MAX_VALUE;
    private long initSequence = 0;
    private long lastSequenceStart = 0;
    private long lastSequenceEnd = 0;

    public QuotesL2Book(Instrument instrument, int l2depth, int size) {
        this.instrument = instrument;
        this.l2depth = l2depth;
        this.pegIndexPrice = instrument.priceToInt(instrument.getPegIndexPrice());
        this.pegIndex = size / 2;
        this.quoteQtys = new IntArrayList(size);
        for (int i = 0; i < size; i++) {
            this.quoteQtys.add(0);
        }
    }

    public boolean isInitialized() {
        return this.initSequence > 0;
    }

    public void add(QuoteSide side, long price, int quantity) {
        add(side, price, quantity, 0, 0);
    }

    public void add(QuoteSide side, long price, int quantity, long sequenceStart, long sequenceEnd) {
        if (side == BID && bestBidPrice == Integer.MIN_VALUE) bestBidPrice = price;
        if (side == ASK && bestAskPrice == Integer.MAX_VALUE) bestAskPrice = price;
        if (outsideDepth(side, price))
            return;

        quoteQtys.set(toIndex(price), side.sign() * Math.abs(quantity));

        if (side == BID) {
            updateBidBounds(price, quantity);
        }
        else if (side == ASK) {
            updateAskBounds(price, quantity);
        }
        if (lastSequenceEnd == 0) initSequence = sequenceStart;
        this.lastSequenceStart = sequenceStart;
        this.lastSequenceEnd = sequenceEnd;
    }

    private void updateBidBounds(long price, int quantity) {
        if (quantity == 0 && bestBidPrice == price) {
            for (int i = 0; quoteQtys.get(toIndex(bestBidPrice)) == 0 && i < quoteQtys.size(); i++) {
                bestBidPrice = bestBidPrice - instrument.tickSizeInt();
            }
        }
        else if(price > bestBidPrice) {
            int ticksJump = (int)((price - bestBidPrice) / instrument.tickSizeInt());
            int indexTrailing = normIndex(toIndex(bestBidPrice) - l2depth);
            for (int i = 0; i < ticksJump; i++) {
                quoteQtys.set(oneRight(indexTrailing), 0);
            }
            bestBidPrice = price;
        }
    }

    private void updateAskBounds(long price, int quantity) {
        if (quantity == 0 && bestAskPrice == price) {
            for (int i = 0; quoteQtys.get(toIndex(bestAskPrice)) == 0 && i < quoteQtys.size(); i++) {
                bestAskPrice = bestAskPrice + instrument.tickSizeInt();
            }
        }
        else if (price < bestAskPrice) {
            int ticksDrop = (int)((bestAskPrice - price) / instrument.tickSizeInt());
            int indexTrailing = normIndex(toIndex(bestAskPrice) + l2depth);
            for (int i = 0; i < ticksDrop; i++) {
                quoteQtys.set(oneLeft(indexTrailing), 0);
            }
            bestAskPrice = price;
        }
    }

    private boolean outsideDepth(QuoteSide side, long price) {
        return  (side == BID && (Math.abs(bestBidPrice - price) / instrument.tickSizeInt() > l2depth)) ||
                (side == ASK && (Math.abs(price - bestAskPrice) / instrument.tickSizeInt() > l2depth));
    }

    public int toIndex(long price) {
        int rawIndex = (int) (pegIndex + (price - pegIndexPrice) / instrument.tickSizeInt());
        return normIndex(rawIndex);
    }

    private int normIndex(int rawIndex) {
        int normRawIndex = rawIndex % quoteQtys.size();
        return normRawIndex < 0 ? quoteQtys.size() + normRawIndex : normRawIndex;
    }

    private int oneLeft(int index) {
        return index > 0 ? index - 1 : quoteQtys.size() - 1;
    }

    private int oneRight(int index) {
        return index + 1 == quoteQtys.size() ? 0 : index + 1;
    }

    public long getBestBid() {
        return bestBidPrice;
    }

    public long getBestAsk() {
        return bestAskPrice;
    }

    public int getBestBidSize() {
        return quoteQtys.get(toIndex(bestBidPrice));
    }

    public int getBestAskSize() {
        return quoteQtys.get(toIndex(bestAskPrice));
    }

    public int getQtyAt(long price) {
        return quoteQtys.get(toIndex(price));
    }

    public int getQtyAt(QuoteSide side, int depth) {
        int index = side == BID ? toIndex(getBestBid()) - depth : toIndex(getBestAsk()) + depth;
        return quoteQtys.get(index);
    }

    public List<float[]> getBids() {
        List<float[]> bids = new ArrayList<>();
        long bidPrice = getBestBid();
        for (int i = toIndex(bidPrice); toIndex(bidPrice) != toIndex(getBestBid() + l2depth); i = oneLeft(i)) {
            bids.add(new float[]{ instrument.priceToDouble(bidPrice), instrument.qtyToDouble(quoteQtys.get(i)) });
            bidPrice -= instrument.tickSizeInt();
        }
        return bids;
    }

    public List<float[]> getAsks() {
        List<float[]> bids = new ArrayList<>();
        long askPrice = getBestAsk();
        for (int i = toIndex(askPrice); toIndex(askPrice) != toIndex(getBestBid() + l2depth); i = oneLeft(i)) {
            bids.add(new float[]{ instrument.priceToDouble(askPrice), instrument.qtyToDouble(quoteQtys.get(i)) });
            askPrice -= instrument.tickSizeInt();
        }
        return bids;
    }

    public long getLastSequenceStart(){
        return this.lastSequenceStart;
    }

    public long getLastSequenceEnd(){
        return this.lastSequenceEnd;
    }
}
