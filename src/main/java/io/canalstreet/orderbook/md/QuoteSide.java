package io.canalstreet.orderbook.md;

public enum QuoteSide {

    BID(1),
    ASK(-1);

    private final int sign;

    QuoteSide(int sign) {
        this.sign = sign;
    }

    public int sign() {
        return this.sign;
    }

    public static QuoteSide fromSign(int sign) {
        if (sign == 0) throw new IllegalArgumentException("Illegal Side sign: 0");
        return sign > 0 ? BID : ASK;
    }

}
