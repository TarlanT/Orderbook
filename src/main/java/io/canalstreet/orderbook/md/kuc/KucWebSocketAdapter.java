package io.canalstreet.orderbook.md.kuc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.canalstreet.orderbook.data.Instrument;
import io.canalstreet.orderbook.data.PerpetualFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

public class KucWebSocketAdapter {

    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private WebSocket webSocketMargin;
    private WebSocket webSocketFutures;
    private static final Map<String, Instrument> subscriptions = HashMap.newHashMap(2);

    public void subscribe(Instrument instrument, String token, BiConsumer<Instrument,JsonNode> messageHandler, Runnable wsClosedHandler) {
        String subscribeMessage = """
                { "id":1,"type":"subscribe","topic":"/spotMarket/level2Depth5:%s", "response":true }
                """.formatted(instrument.getSymbol());

        if (instrument instanceof PerpetualFuture) {
            if (webSocketFutures == null || webSocketFutures.isInputClosed()) {
                webSocketFutures = httpClient.newWebSocketBuilder().buildAsync(URI.create("wss://ws-api-futures.kucoin.com?token="+token),
                        new BncWebSocketListener(messageHandler, wsClosedHandler)).join();
            }
            webSocketFutures.sendText(subscribeMessage, true);
            subscriptions.put("/contractMarket/level2Depth5:"+instrument.getSymbol(), instrument);
        } else {
            if (webSocketMargin == null || webSocketMargin.isInputClosed()) {
                webSocketMargin = httpClient.newWebSocketBuilder().buildAsync(URI.create("wss://ws-api-spot.kucoin.com?token="+token),
                        new BncWebSocketListener(messageHandler, wsClosedHandler)).join();
            }
            webSocketMargin.sendText(subscribeMessage, true);
            subscriptions.put("/spotMarket/level2Depth5:"+instrument.getSymbol(), instrument);
        }
    }

    public void unsubscribe(Instrument instrument) {
        String unSubscribeMessage = """
                { "id":1, "type":"unsubscribe", "topic":"/spotMarket/level2Depth5:%s", "response":true }
                """.formatted(instrument.getSymbol());

        if (instrument instanceof PerpetualFuture && webSocketFutures != null && !webSocketFutures.isInputClosed()) {
            webSocketFutures.sendText(unSubscribeMessage, true);
        }
        else if (webSocketMargin != null && !webSocketMargin.isInputClosed()) {
            webSocketMargin.sendText(unSubscribeMessage, true);
        }
    }

    static class BncWebSocketListener implements WebSocket.Listener {
        private static final Logger LOGGER = LoggerFactory.getLogger(BncWebSocketListener.class);
        private static final ObjectMapper OM = new ObjectMapper();
        private final BiConsumer<Instrument,JsonNode> messageHandler;
        private final Runnable wsClosedHandler;

        public BncWebSocketListener(BiConsumer<Instrument,JsonNode> messageHandler, Runnable wsClosedCallback) {
            this.messageHandler = messageHandler;
            this.wsClosedHandler = wsClosedCallback;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            WebSocket.Listener.super.onOpen(webSocket);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            if (messageHandler != null) {
                try {
                    JsonNode message = OM.readTree(data.toString());
                    if ("message".equals(message.get("type").asText())) {
                        String topic = message.get("topic").asText();
                        messageHandler.accept(subscriptions.get(topic), message);
                    }
                } catch (Exception ex) {
                    LOGGER.error("Failed to parse message: {}", data, ex);
                }
            }
            return null;
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            return WebSocket.Listener.super.onBinary(webSocket, data, last);
        }

        @Override
        public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
            try {
                LOGGER.info("Received ping message: {}", message);
                ObjectNode pong = ((ObjectNode)OM.readTree(message.array())).put("type", "pong");
                ByteBuffer byteBuffer = ByteBuffer.wrap(OM.writeValueAsBytes(pong));
                LOGGER.info("Sending pong message: {}", byteBuffer);
                webSocket.sendPong(byteBuffer);
            } catch (IOException ex) {
                LOGGER.error("Failed to parse ping message: {}", message, ex);
            }
            return null;
        }

        @Override
        public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
            return WebSocket.Listener.super.onPong(webSocket, message);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            LOGGER.warn("WebSocket closed. Status code: {}. Reason: {}", statusCode, reason);
            if (this.wsClosedHandler != null) {
                LOGGER.info("Reconnecting...");
                this.wsClosedHandler.run();
            }
            return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            WebSocket.Listener.super.onError(webSocket, error);
        }
    }
}
