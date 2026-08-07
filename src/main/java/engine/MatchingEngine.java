package engine;

import model.Order;
import model.Side;
import model.Trade;
import util.IDGenerator;

import java.util.*;

public class MatchingEngine implements BookView {

    private final TreeMap<Long, Deque<Order>> bids = new TreeMap<>(Comparator.reverseOrder());

    private final TreeMap<Long, Deque<Order>> asks = new TreeMap<>();

    private final List<Trade> trades = new ArrayList<>();

    private final Map<Long, Order> openOrders = new HashMap<>();

    private ExecutionListener executionListener = ExecutionListener.NO_OP;

    /** Wire the outbound adapter in Step 7.5; defaults to NO_OP for the console/demo path. */
    public void setExecutionListener(ExecutionListener listener) {
        this.executionListener = (listener == null) ? ExecutionListener.NO_OP : listener;
    }

    private void matchBuy(Order buyOrder) {
        while (buyOrder.getQuantity() > 0 && !asks.isEmpty()) {
            Map.Entry<Long, Deque<Order>> bestAskEntry  = asks.firstEntry();
            long askPrice = bestAskEntry.getKey();

            if (askPrice > buyOrder.getPrice()) break;

            Deque<Order> bestAskQueue = bestAskEntry.getValue();
            Order askOrder = bestAskQueue.getFirst();

            int quantityMatched = Math.min(buyOrder.getQuantity(), askOrder.getQuantity());
            buyOrder.fill(quantityMatched);
            askOrder.fill(quantityMatched);

            long tradeID = IDGenerator.nextTradeID();
            trades.add(new Trade(
                    tradeID,
                    buyOrder.getOrderID(),
                    askOrder.getOrderID(),
                    buyOrder.getParticipantID(),
                    askOrder.getParticipantID(),
                    askPrice,
                    quantityMatched,
                    System.nanoTime()
            ));
            executionListener.onFill(
                    buyOrder.getOrderID(),   // aggressor
                    askOrder.getOrderID(),   // passive
                    tradeID,
                    askPrice,
                    quantityMatched,
                    buyOrder.getQuantity()   // aggressor remaining after this fill
            );

            if (askOrder.getQuantity() == 0) {
                bestAskQueue.removeFirst();
                openOrders.remove(askOrder.getOrderID());   // passive fully consumed — no longer cancellable
                if (bestAskQueue.isEmpty()) {
                    asks.pollFirstEntry();
                }
            }
        }
    }

    private void matchSell(Order sellOrder) {
        while (sellOrder.getQuantity() > 0 && !bids.isEmpty()) {
            Map.Entry<Long, Deque<Order>> bestBidEntry = bids.firstEntry();
            long bidPrice = bestBidEntry.getKey();

            if (bidPrice < sellOrder.getPrice()) break;

            Deque<Order> bestBidQueue = bestBidEntry.getValue();
            Order bidOrder = bestBidQueue.getFirst();

            int quantityMatched = Math.min(sellOrder.getQuantity(), bidOrder.getQuantity());
            sellOrder.fill(quantityMatched);
            bidOrder.fill(quantityMatched);

            long tradeID = IDGenerator.nextTradeID();
            trades.add(new Trade(
                    tradeID,
                    bidOrder.getOrderID(),
                    sellOrder.getOrderID(),
                    bidOrder.getParticipantID(),
                    sellOrder.getParticipantID(),
                    bidPrice,
                    quantityMatched,
                    System.nanoTime()
            ));
            executionListener.onFill(
                    sellOrder.getOrderID(),  // aggressor
                    bidOrder.getOrderID(),   // passive
                    tradeID,
                    bidPrice,
                    quantityMatched,
                    sellOrder.getQuantity()  // aggressor remaining after this fill
            );

            if (bidOrder.getQuantity() == 0) {
                bestBidQueue.removeFirst();
                openOrders.remove(bidOrder.getOrderID());   // passive fully consumed — no longer cancellable
                if (bestBidQueue.isEmpty()) {
                    bids.pollFirstEntry();
                }
            }
        }
    }

    public void addOrder(Order order) {
        if (order.getSide() == Side.BUY) {
            matchBuy(order);
            if (order.getQuantity() > 0) {
                addToBook(bids, order);
                executionListener.onAccepted(order.getOrderID(), order.getPrice(), order.getQuantity());
            }
        } else {
            matchSell(order);
            if (order.getQuantity() > 0) {
                addToBook(asks, order);
                executionListener.onAccepted(order.getOrderID(), order.getPrice(), order.getQuantity());
            }
        }
    }

    private void addToBook(TreeMap<Long, Deque<Order>> book, Order order) {
        book.computeIfAbsent(order.getPrice(), k -> new ArrayDeque<>()).addLast(order);
        openOrders.put(order.getOrderID(), order);   // register resting order for O(1) cancel
    }

    /**
     * @return true if a live resting order was found and cancelled; false for an
     *         unknown id or an order already fully filled. The outbound adapter
     *         maps this to ORDER_CANCELLED vs ORDER_REJECTED.
     */
    public boolean cancelOrder(long orderID) {
        Order order = openOrders.remove(orderID);
        if (order == null || order.getQuantity() == 0) return false;

        order.cancel();

        TreeMap<Long, Deque<Order>> book = (order.getSide() == Side.BUY) ? bids : asks;
        Deque<Order> queue = book.get(order.getPrice());
        if (queue != null) {
            queue.remove(order);
            if (queue.isEmpty()) book.remove(order.getPrice());
        }
        return true;
    }

    public List<Trade> getTrades() {
        return trades;
    }

    public void printBook() {
        System.out.println("=============== ORDER BOOK ===============");

        // Asks: highest at top, lowest nearest to spread
        for (Map.Entry<Long, Deque<Order>> entry : asks.descendingMap().entrySet()) {
            int totalQty = entry.getValue().stream().mapToInt(Order::getQuantity).sum();
            System.out.printf("  ASK  %10s   %6d%n", formatPrice(entry.getKey()), totalQty);
        }

        long bestBid = getBestBid();
        long bestAsk = getBestAsk();
        System.out.println("  ---------------------------------------");
        if (bestBid != -1L && bestAsk != -1L) {
            long spread = bestAsk - bestBid;
            long midpoint = (bestBid + bestAsk) / 2;
            System.out.printf("  Spread: %s     Mid: %s%n", formatPrice(spread), formatPrice(midpoint));
        } else {
            System.out.println("  No spread (one side empty)");
        }
        System.out.println("  ---------------------------------------");

        // Bids: highest first (natural iteration of reverse-ordered TreeMap)
        for (Map.Entry<Long, Deque<Order>> entry : bids.entrySet()) {
            int totalQty = entry.getValue().stream().mapToInt(Order::getQuantity).sum();
            System.out.printf("  BID  %10s   %6d%n", formatPrice(entry.getKey()), totalQty);
        }

        System.out.println("==========================================");
    }

    private String formatPrice(long priceInCents) {
        return String.format("$%d.%02d", priceInCents / 100, priceInCents % 100);
    }

    @Override
    public long getBestBid() {
        return bids.firstKey();
    }

    @Override
    public long getBestAsk() {
        return asks.firstKey();
    }
}