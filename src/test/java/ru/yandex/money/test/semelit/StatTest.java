package ru.yandex.money.test.semelit;

import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StatTest {

    static Method insertAtMethod;
    static Method countInDurationMethod;
    static Field offsetStampField;

    @BeforeClass
    public static void init() throws NoSuchFieldException {
        insertAtMethod = Arrays.stream(EventStat.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("insertAt"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Not found EventStat.insertAt"));
        insertAtMethod.setAccessible(true);

        countInDurationMethod = Arrays.stream(EventStat.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("countInDuration"))
                .findFirst()
                .orElseThrow(() ->  new RuntimeException("Not found EventStat.countInDuration"));
        countInDurationMethod.setAccessible(true);

        offsetStampField = EventStat.class.getDeclaredField("offsetStamp");
        offsetStampField.setAccessible(true);
    }

    private static void assertNInLastMinute(int n, EventStat<?> es) {
        int countInLastMinute = es.countInLastMinute();
        assertTrue("Ожидаем " + n + " за последнюю минуту, но " + countInLastMinute, countInLastMinute == n);
    }

    private static void assertNInLastMinuteAtTimestamp(int n, EventStat<?> es, long timestamp) throws InvocationTargetException, IllegalAccessException {
        int countInLastMinute = (Integer) countInDurationMethod.invoke(es, EventStat.SECONDS_IN_MINUTE, timestamp);
        assertTrue("Ожидаем " + n + " за последнюю минуту, но " + countInLastMinute, countInLastMinute == n);
    }


    private static void assertNInLastHour(int n, EventStat<?> es) {
        int countInLastHour = es.countInLastHour();
        assertTrue("Ожидаем " + n + " за последний час, но " + countInLastHour, countInLastHour == n);
    }

    private static void assertNInLastHourAtTimestamp(int n, EventStat<?> es, long timestamp) throws InvocationTargetException, IllegalAccessException {
        int countInLastHour = (Integer) countInDurationMethod.invoke(es, EventStat.SECONDS_IN_HOUR, timestamp);
        assertTrue("Ожидаем " + n + " за последний час, но " + countInLastHour, countInLastHour == n);
    }


    private static void assertNInLastDay(int n, EventStat<?> es) {
        int countInLastDay = es.countInLastDay();
        assertTrue("Ожидаем " + n + " за последний день, но " + countInLastDay, countInLastDay == n);
    }

    private static void assertNInLastDayAtTimestamp(int n, EventStat<?> es, long timestamp) throws InvocationTargetException, IllegalAccessException {
        int countInLastDay = (Integer) countInDurationMethod.invoke(es, EventStat.SECONDS_IN_24_HOURS, timestamp);
        assertTrue("Ожидаем " + n + " за последний день, но " + countInLastDay, countInLastDay == n);
    }

    private static <T> Object insertAtStamp(T event, EventStat<T> es, long timestamp) throws InvocationTargetException, IllegalAccessException {
        return insertAtMethod.invoke(es, event, timestamp);
    }

    private static long getOffsetStamp(EventStat<?> es) throws IllegalAccessException {
        return (Long) offsetStampField.get(es);
    }

    @Test
    public void testTrivialInsert() {
        EventStat<Object> es = new EventStat<>();
        Object event = new Object();
        Object insertedEvent = es.insert(event);
        assertTrue("Ожидаем то же событие после вставки, но " + insertedEvent, event == insertedEvent);
    }

    @Test
    public void testInsertInThePast() throws InvocationTargetException, IllegalAccessException {
        EventStat<Object> es = new EventStat<>();
        assertNull("Ожидаем null при попытке вставить событие в прошлом", insertAtStamp(new Object(), es, System.currentTimeMillis() - 60*1000));
    }


    @Test
    public void testTrivialCountInLastMinute() {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertNInLastMinute(1, es);
    }

    @Test
    public void testCountInThePast() throws InvocationTargetException, IllegalAccessException {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertNInLastMinuteAtTimestamp(-1, es, System.currentTimeMillis() - 60*1000);
    }


    @Test
    public void testTrivialCountInLastHour() {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertNInLastHour(1, es);
    }

    @Test
    public void testTrivialCountInLastDay() {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertNInLastDay(1, es);
    }

    @Test
    public void testTrivial10KCount() {
        EventStat<Object> es = new EventStat<>();
        int n = 10_000;
        for (int i = 0; i < n;i++) {
            es.insert(new Object());
        }
        assertNInLastMinute(n, es);
        assertNInLastHour(n, es);
        assertNInLastDay(n, es);
    }


    @Test
    public void testTrivialInLastHourNotInLastMinute() throws InvocationTargetException, IllegalAccessException {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertNInLastMinute(1, es);
        assertNInLastHour(1, es);
        long nextMinuteStamp = System.currentTimeMillis() + 1000 * 62;
        assertNInLastMinuteAtTimestamp(0, es, nextMinuteStamp);
        assertNInLastHourAtTimestamp(1, es, nextMinuteStamp);
    }

    @Test
    public void testTrivialInLastDayNotInLastHour() throws InvocationTargetException, IllegalAccessException {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertNInLastMinute(1, es);
        assertNInLastHour(1, es);
        assertNInLastDay(1, es);
        long nextHourStamp = System.currentTimeMillis() + 1000 * 36600; // 1h 10m
        assertNInLastMinuteAtTimestamp(0, es, nextHourStamp);
        assertNInLastHourAtTimestamp(0, es, nextHourStamp);
        assertNInLastDayAtTimestamp(1, es, nextHourStamp);
    }

    @Test
    public void testTrivialOverflow() throws InvocationTargetException, IllegalAccessException {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertNInLastMinute(1, es);
        assertNInLastHour(1, es);
        assertNInLastDay(1, es);
        long nextHourStamp = System.currentTimeMillis() + EventStat.SECONDS_IN_24_HOURS * 1000 + EventStat.SECONDS_IN_HOUR * 1000; // 25h
        assertNInLastMinuteAtTimestamp(0, es, nextHourStamp);
        assertNInLastHourAtTimestamp(0, es, nextHourStamp);
        assertNInLastDayAtTimestamp(0, es, nextHourStamp);
    }

    @Test
    public void testInsertAfterOverflow() throws InvocationTargetException, IllegalAccessException {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertNInLastMinute(1, es);
        assertNInLastHour(1, es);
        assertNInLastDay(1, es);
        long nextHourStamp = System.currentTimeMillis() + EventStat.SECONDS_IN_24_HOURS * 1000 + 1000; // 24h 1s
        assertNInLastMinuteAtTimestamp(0, es, nextHourStamp);
        assertNInLastHourAtTimestamp(0, es, nextHourStamp);
        assertNInLastDayAtTimestamp(0, es, nextHourStamp);
        insertAtStamp(new Object(), es, nextHourStamp);
        assertNInLastMinuteAtTimestamp(1, es, nextHourStamp);
        assertNInLastHourAtTimestamp(1, es, nextHourStamp);
        assertNInLastDayAtTimestamp(1, es, nextHourStamp);
    }

    @Test
    public void test10KConcurrentInsert() throws ExecutionException, InterruptedException {
        final EventStat<Object> es = new EventStat<>();

        int n = 10_000;
        List<ForkJoinTask<Void>> taskList = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            taskList.add(new RecursiveAction() {
                @Override
                protected void compute() {
                    es.insert(new Object());
                }
            }.fork());
        }

        while (!taskList.isEmpty()) {
            taskList.remove(0).get();
        }

        assertNInLastMinute(n, es);
        assertNInLastHour(n, es);
        assertNInLastDay(n, es);
    }

    @Test
    public void testConcurrentOverflow() throws ExecutionException, InterruptedException, IllegalAccessException, InvocationTargetException {
        final EventStat<Object> es = new EventStat<>();
        final long offsetStamp = getOffsetStamp(es);
        int n = 10_000;
        long finalStamp = offsetStamp + EventStat.MILLIS_IN_24_HOURS + n/2 + 1;
        List<ForkJoinTask<Void>> taskList = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final long nextStamp = offsetStamp + EventStat.MILLIS_IN_24_HOURS - n/2 + 1 + i;
            taskList.add(new RecursiveAction() {
                @Override
                protected void compute() {
                    try {
                        insertAtStamp(new Object(), es, nextStamp);
                    } catch (InvocationTargetException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }.fork());
        }

        while (!taskList.isEmpty()) {
            taskList.remove(0).get();
        }

        assertNInLastMinuteAtTimestamp(n/2, es, finalStamp);
        assertNInLastHourAtTimestamp(n/2, es, finalStamp);
        assertNInLastDayAtTimestamp(n/2, es, finalStamp);
    }


}
