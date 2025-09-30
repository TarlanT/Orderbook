package io.canalstreet.orderbook.md;

import io.canalstreet.orderbook.data.Instrument;
import io.canalstreet.orderbook.md.kuc.KucMarketDataService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class OrderbookRestController {

    @Autowired
    KucMarketDataService kucMarketDataService;

    private Instrument instrument;

    @PostConstruct
    public void initSubscriptions() {
        instrument = Instrument.builder().symbol("BTC-USDT").currency("USDT").quantityScale(8).priceScale(1).tickSize(0.1f).pegIndexPrice(100_000).build();
        kucMarketDataService.subscribe(instrument);
    }

    @RequestMapping(value = "/orderbook", produces = {"application/json"})
    public Object getOrderbook() {
        Map<String, List<float[]>> book = new HashMap<>();
        book.put("bids", kucMarketDataService.getQuotesL2(instrument).getBids());
        book.put("asks", kucMarketDataService.getQuotesL2(instrument).getAsks());
        return book;
    }

}
