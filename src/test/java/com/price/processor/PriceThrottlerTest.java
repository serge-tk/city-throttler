package com.price.processor;

import net.jodah.concurrentunit.Waiter;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class PriceThrottlerTest {
    private final static String EURUSD = "EURUSD";
    private final static String USDJPY = "USDJPY";

    private static class TestPriceProcessor implements PriceProcessor {
        public final Map<String, Double> rates = new HashMap<>();
        private final Runnable action;
        private final long delay;

        public TestPriceProcessor(long delay, Runnable action) {
            this.delay = delay;
            this.action = action;
        }

        @Override
        synchronized public void onPrice(String ccyPair, double rate) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException ignored) {
            }
            rates.put(ccyPair, rate);
            action.run();
        }

        @Override
        public void subscribe(PriceProcessor priceProcessor) {
        }

        @Override
        public void unsubscribe(PriceProcessor priceProcessor) {
        }
    }

    @Test
    public void testWorksWithoutSubscribersAndSentPricesAfterSubscription() throws Exception {
        PriceThrottler throttler = new PriceThrottler();
        throttler.onPrice(USDJPY, 1);
        throttler.onPrice(EURUSD, 2);
        Waiter waiter = new Waiter();

        // subscriber should receive all available prices on startup
        TestPriceProcessor subscriber = new TestPriceProcessor(0, waiter::resume);
        throttler.subscribe(subscriber);
        waiter.await(1000, TimeUnit.MILLISECONDS, 2);
        assertEquals(Double.valueOf(1), subscriber.rates.get(USDJPY));
        assertEquals(Double.valueOf(2), subscriber.rates.get(EURUSD));
    }

    @Test
    public void testDoNotNotifyAboutSameRate() throws Exception {
        PriceThrottler throttler = new PriceThrottler();
        Waiter waiter = new Waiter();
        TestPriceProcessor subscriber = new TestPriceProcessor(100, waiter::resume);
        throttler.subscribe(subscriber);

        throttler.onPrice(USDJPY, 1);
        throttler.onPrice(USDJPY, 2);
        throttler.onPrice(USDJPY, 1);
        waiter.await(1000, TimeUnit.MILLISECONDS, 1);
        assertEquals(Double.valueOf(1), subscriber.rates.get(USDJPY));
        throttler.onPrice(USDJPY, 3);
        waiter.await(1000);
        assertEquals(Double.valueOf(3), subscriber.rates.get(USDJPY));
    }

    @Test
    public void testSlowAndFastSubscribersInteraction() throws Exception {
        PriceThrottler throttler = new PriceThrottler();
        Waiter waiter = new Waiter();

        TestPriceProcessor slowSubscriber = new TestPriceProcessor(100, waiter::resume);
        throttler.subscribe(slowSubscriber);
        TestPriceProcessor fastSubscriber = new TestPriceProcessor(0, waiter::resume);
        throttler.subscribe(fastSubscriber);

        throttler.onPrice(EURUSD, 1);
        Thread.sleep(2);
        throttler.onPrice(EURUSD, 2);
        Thread.sleep(2);
        throttler.onPrice(EURUSD, 3);
        Thread.sleep(2);

        waiter.await(100, TimeUnit.MILLISECONDS, 3);
        assertEquals(Double.valueOf(3), fastSubscriber.rates.get(EURUSD));

        waiter.await(1000);
        assertEquals(Double.valueOf(1), slowSubscriber.rates.get(EURUSD));

        waiter.await(1000);
        assertEquals(Double.valueOf(3), slowSubscriber.rates.get(EURUSD));
    }
}
