package io.canalstreet.orderbook.data;

import io.canalstreet.orderbook.md.kuc.KucMarketDataService;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

@Data
@SuperBuilder(toBuilder = true)
public class Instrument {

    private long id;

    private String symbol;

    private String fullName;

    private KucMarketDataService mdFeed;

    private Instrument underlying;

    @Builder.Default
    private int leverage = 1;

    private String currency;

    @Builder.Default
    private String portfolioCurrency = "USD";

    @Builder.Default
    private int priceScale = 2;

    @Builder.Default
    private int quantityScale = 2;

    @Builder.Default
    private float tickSize = 0.01f;

    private float pegIndexPrice;

    public int qtyToInt(double quantity) {
        return (int)Math.round(quantity * Math.pow(10, quantityScale));
    }

    public int qtyToDouble(long quantity) {
        return (int)Math.round(quantity / Math.pow(10, quantityScale));
    }

    public long priceToInt(double price) {
        return Math.round(price * Math.pow(10, priceScale));
    }

    public long priceToDouble(long price) {
        return Math.round(price / Math.pow(10, priceScale));
    }

    public int tickSizeInt() {
        return (int)Math.round(tickSize * Math.pow(10, priceScale));
    }
}