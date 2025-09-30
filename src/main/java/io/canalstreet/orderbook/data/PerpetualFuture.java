package io.canalstreet.orderbook.data;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;


@Data
@EqualsAndHashCode(callSuper = true)
@SuperBuilder(toBuilder = true)
public class PerpetualFuture extends Instrument {

    private double fundingRate;

}
