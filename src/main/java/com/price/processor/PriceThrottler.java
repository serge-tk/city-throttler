package com.price.processor;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The most generic implementation
 *
 * 1. All subscribers is provisioned by last available rates after subscription immediately. If this isn't necessary it can be changed by swapping of notifyIfChanged() and priceUpdateMitex.wait()
 * 2. Each subscriber receives rate updates in the single thread so it shouldn't worry about concurrent updates
 * 3. Subscribers won't receive same rate twice consequentially even they are missed a spike.
 * 4. Throttler is able to receive updates from different threads (for example from several independent sources)
 * 5. Throttler is fair. It doesn't miss rarely changing pairs even other ones are changing frequently.
 */

public class PriceThrottler implements PriceProcessor {
    private final Map<String, Double> ratesByCcyPair = new ConcurrentHashMap<>();
    private final Map<PriceProcessor, Subscription> subscriptions = new IdentityHashMap<>();
    private final Object priceUpdateMutex = new Object();
    private final AtomicLong timestamp = new AtomicLong(0L);

    private class Subscription {
        private final PriceProcessor subscriber;
        private final Map<String, Double> lastSentRatesByCcyPair = new ConcurrentHashMap<>();
        private Thread processingThread;

        public Subscription(PriceProcessor subscriber) {
            this.subscriber = subscriber;
        }

        public void startProcessing() {
            this.processingThread = new Thread(() -> {
                while (!Thread.interrupted()) {
                    long latestTimestamp = timestamp.longValue();
                    for (String ccyPair : ratesByCcyPair.keySet()) {
                        if (Thread.interrupted()) {
                            return;
                        }
                        notifyIfChanged(ccyPair, ratesByCcyPair.get(ccyPair));
                    }
                    if (latestTimestamp == timestamp.longValue()) {
                        synchronized (priceUpdateMutex) {
                            try {
                                priceUpdateMutex.wait();
                            } catch (InterruptedException e) {
                                break;
                            }
                        }
                    }
                }
            });
            processingThread.start();
        }

        public void stopProcessing() {
            processingThread.interrupt();
        }

        private void notifyIfChanged(String ccyPair, double rate) {
            Double lastRate = lastSentRatesByCcyPair.get(ccyPair);
            if (lastRate == null || lastRate != rate) {
                try {
                    subscriber.onPrice(ccyPair, rate);
                } catch (Exception ignored) {
                    // ignore the exception and don't try to send same rate value again
                }
                lastSentRatesByCcyPair.put(ccyPair, rate);
            }
        }
    }

    public void shutdown() {
        synchronized (subscriptions) {
            for (Subscription subscription : subscriptions.values()) {
                subscription.stopProcessing();
            }
            subscriptions.clear();
        }
    }

    @Override
    public void onPrice(String ccyPair, double rate) {
        ratesByCcyPair.put(ccyPair, rate);
        timestamp.incrementAndGet();
        synchronized (priceUpdateMutex) {
            priceUpdateMutex.notifyAll();
        }
    }

    @Override
    public void subscribe(PriceProcessor priceProcessor) {
        synchronized (subscriptions) {
            subscriptions.computeIfAbsent(priceProcessor, subscriber -> {
                Subscription subscription = new Subscription(subscriber);
                subscription.startProcessing();
                return subscription;
            });
        }
    }

    @Override
    public void unsubscribe(PriceProcessor priceProcessor) {
        synchronized (subscriptions) {
            Optional.ofNullable(subscriptions.remove(priceProcessor)).ifPresent(Subscription::stopProcessing);
        }
    }
}
