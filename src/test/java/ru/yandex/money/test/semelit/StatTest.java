package ru.yandex.money.test.semelit;

import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class StatTest {

    static Method insertAt;
    static Method countInDuration;

    @BeforeClass
    public static void init() {
        insertAt = Arrays.stream(EventStat.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("insertAt"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Not found EventStat.insertAt"));
        insertAt.setAccessible(true);

        countInDuration = Arrays.stream(EventStat.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("countInDuration"))
                .findFirst()
                .orElseThrow(() ->  new RuntimeException("Not found EventStat.countInDuration"));
        countInDuration.setAccessible(true);
    }

    private static void assertNInLastMinute(int n, EventStat<?> es) {
        int countInLastMinute = es.countInLastMinute();
        assertTrue("Ожидаем " + n + " за последнюю минуту, но " + countInLastMinute, countInLastMinute == n);
    }

    private static void assertNInLastMinuteAtTimestamp(int n, EventStat<?> es, long timestamp) throws InvocationTargetException, IllegalAccessException {
        int countInLastMinute = (Integer) countInDuration.invoke(es, EventStat.SECONDS_IN_MINUTE, timestamp);
        assertTrue("Ожидаем " + n + " за последнюю минуту, но " + countInLastMinute, countInLastMinute == n);
    }


    private static void assertNInLastHour(int n, EventStat<?> es) {
        int countInLastHour = es.countInLastHour();
        assertTrue("Ожидаем " + n + " за последний час, но " + countInLastHour, countInLastHour == n);
    }

    private static void assertNInLastHourAtTimestamp(int n, EventStat<?> es, long timestamp) throws InvocationTargetException, IllegalAccessException {
        int countInLastHour = (Integer) countInDuration.invoke(es, EventStat.SECONDS_IN_HOUR, timestamp);
        assertTrue("Ожидаем " + n + " за последний час, но " + countInLastHour, countInLastHour == n);
    }


    private static void assertNInLastDay(int n, EventStat<?> es) {
        int countInLastDay = es.countInLastDay();
        assertTrue("Ожидаем " + n + " за последний день, но " + countInLastDay, countInLastDay == n);
    }

    private static void assertNInLastDayAtTimestamp(int n, EventStat<?> es, long timestamp) throws InvocationTargetException, IllegalAccessException {
        int countInLastDay = (Integer) countInDuration.invoke(es, EventStat.SECONDS_IN_24_HOURS, timestamp);
        assertTrue("Ожидаем " + n + " за последний день, но " + countInLastDay, countInLastDay == n);
    }

    private static <T> void insertAtStamp(T event, EventStat<T> es, long timestamp) throws InvocationTargetException, IllegalAccessException {
        insertAt.invoke(es, event, timestamp);
    }

    @Test
    public void testTrivialInsert() {
        EventStat<Object> es = new EventStat<>();
        Object event = new Object();
        Object insertedEvent = es.insert(event);
        assertTrue("Ожидаем то же событие после вставки, но " + insertedEvent, event == insertedEvent);
    }

    @Test
    public void testTrivialCountInLastMinute() {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertNInLastMinute(1, es);
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



}
