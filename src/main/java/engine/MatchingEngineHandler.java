package engine;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import event.ExecutionEvent;
import event.ExecutionEventType;
import event.OrderEvent;
import event.OrderEventType;
import model.Order;

import java.util.function.LongSupplier;

/**
 * Adapts MatchingEngine onto the Disruptor: consumes OrderEvent from the inbound
 * ring buffer and produces ExecutionEvent onto the outbound ring buffer. Sole
 * outbound producer (SRS §3.3), so the outbound ring is ProducerType.SINGLE.
 *
 * onEvent builds the Order from the OrderEvent — THIS is where domain validation
 * fires (the Order constructor throws on bad fields), since the parser
 * deliberately never builds an Order. Match executions are reported back via the
 * ExecutionListener callbacks and published from there.
 *
 * The injectable clock mirrors the OrderGateway seam (Step 7) for deterministic
 * tests and future JMH timestamping (SRS §6.3).
 */
public final class MatchingEngineHandler implements EventHandler<OrderEvent>, ExecutionListener {

    /** Demo simplification: the FIX subset carries no participant; the gateway is stateless. */
    private static final long GATEWAY_PARTICIPANT_ID = 1L;
    private static final long NA = -1L;

    private final MatchingEngine engine;
    private final RingBuffer<ExecutionEvent> outbound;
    private final LongSupplier clock;

    public MatchingEngineHandler(MatchingEngine engine, RingBuffer<ExecutionEvent> outbound) {
        this(engine, outbound, System::nanoTime);
    }

    // Package-private clock seam for tests.
    MatchingEngineHandler(MatchingEngine engine, RingBuffer<ExecutionEvent> outbound, LongSupplier clock) {
        this.engine = engine;
        this.outbound = outbound;
        this.clock = clock;
    }

    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) {
        if (event.eventType == OrderEventType.CANCEL_ORDER) {
            boolean cancelled = engine.cancelOrder(event.originalOrderId);
            publish(cancelled ? ExecutionEventType.ORDER_CANCELLED : ExecutionEventType.ORDER_REJECTED,
                    event.originalOrderId, NA, NA, NA, NA, NA, NA);
            return;
        }

        // NEW_ORDER: domain validation fires here, in the Order constructor.
        Order order;
        try {
            order = new Order(
                    event.orderId,
                    event.timestamp,
                    event.side,
                    (int) event.quantity,
                    event.price,
                    GATEWAY_PARTICIPANT_ID
            );
        } catch (IllegalArgumentException rejected) {
            publish(ExecutionEventType.ORDER_REJECTED, event.orderId, NA, NA, NA, NA, NA, NA);
            return;
        }

        engine.addOrder(order);   // drives matching; fires onFill / onAccepted below
    }

    // --- ExecutionListener: fired synchronously by the engine during addOrder ---

    @Override
    public void onFill(long aggressorOrderId, long passiveOrderId, long tradeId,
                       long price, long filledQuantity, long aggressorRemainingQuantity) {
        ExecutionEventType type = (aggressorRemainingQuantity == 0)
                ? ExecutionEventType.ORDER_FILLED
                : ExecutionEventType.ORDER_PARTIALLY_FILLED;
        publish(type, aggressorOrderId, tradeId, price, filledQuantity,
                aggressorRemainingQuantity, aggressorOrderId, passiveOrderId);
    }

    @Override
    public void onAccepted(long orderId, long price, long remainingQuantity) {
        publish(ExecutionEventType.ORDER_ACCEPTED, orderId, NA, price, NA, remainingQuantity, NA, NA);
    }

    // --- single outbound publish point: writes EVERY field (slot-reuse discipline) ---

    private void publish(ExecutionEventType type, long orderId, long tradeId, long price,
                         long filledQuantity, long remainingQuantity,
                         long aggressorOrderId, long passiveOrderId) {
        long seq = outbound.next();
        try {
            ExecutionEvent e = outbound.get(seq);
            e.eventType         = type;
            e.orderId           = orderId;
            e.tradeId           = tradeId;
            e.price             = price;
            e.filledQuantity    = filledQuantity;
            e.remainingQuantity = remainingQuantity;
            e.aggressorOrderId  = aggressorOrderId;
            e.passiveOrderId    = passiveOrderId;
            e.timestamp         = clock.getAsLong();
        } finally {
            outbound.publish(seq);
        }
    }
}