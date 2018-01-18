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

import static org.junit.Assert.*;

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

    private static <T> boolean insertAtStamp(T event, EventStat<T> es, long timestamp) throws InvocationTargetException, IllegalAccessException {
        return (Boolean) insertAtMethod.invoke(es, event, timestamp);
    }

    private static long getOffsetStamp(EventStat<?> es) throws IllegalAccessException {
        return (Long) offsetStampField.get(es);
    }

    @Test
    public void testTrivialInsert() {
        EventStat<Object> es = new EventStat<>();
        Object event = new Object();
        assertTrue(es.insert(event));
    }

    @Test
    public void testInsertInThePast() throws InvocationTargetException, IllegalAccessException {
        EventStat<Object> es = new EventStat<>();
        assertFalse("Ожидаем false при попытке вставить событие в прошлом", insertAtStamp(new Object(), es, System.currentTimeMillis() - EventStat.MILLIS_IN_24_HOURS - 60*1000));
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
    public void testTrivialNotInLastDay() throws InvocationTargetException, IllegalAccessException {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertNInLastMinute(1, es);
        assertNInLastHour(1, es);
        assertNInLastDay(1, es);
        long nextDayStamp = System.currentTimeMillis() + EventStat.MILLIS_IN_24_HOURS; // ~24h
        assertNInLastMinuteAtTimestamp(0, es, nextDayStamp);
        assertNInLastHourAtTimestamp(0, es, nextDayStamp);
        assertNInLastDayAtTimestamp(0, es, nextDayStamp);
    }

    @Test
    public void testInsertAfterOverflow() throws InvocationTargetException, IllegalAccessException {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertNInLastMinute(1, es);
        assertNInLastHour(1, es);
        assertNInLastDay(1, es);
        long nextHourStamp = System.currentTimeMillis() + EventStat.MILLIS_IN_24_HOURS + 1000; // 24h 1s
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
    public void test10KConcurrentInsertWithLag() throws ExecutionException, InterruptedException {
        final EventStat<Object> es = new EventStat<>();

        int n = 10_000;
        List<ForkJoinTask<Void>> taskList = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            taskList.add(new RecursiveAction() {
                @Override
                protected void compute() {
                    es.insert(new Object());
                }
            }.fork());
            if (i % 5001 == 0) { //таким образом ограничеваем кол-во событий
                while (!taskList.isEmpty()) {
                    taskList.remove(0).get();
                }
                Thread.sleep(1990); //эмулируем лаг на точке пересечения секунд
            }

        }

        while (!taskList.isEmpty()) {
            taskList.remove(0).get();
        }

        assertNInLastMinute(n, es);
        assertNInLastHour(n, es);
        assertNInLastDay(n, es);
    }



    @Test
    public void testConcurrentAtOverflow() throws ExecutionException, InterruptedException, IllegalAccessException, InvocationTargetException {
        final EventStat<Object> es = new EventStat<>();
        final long offsetStamp = getOffsetStamp(es);
        int n = 5_000;
        long finalStamp = offsetStamp + EventStat.MILLIS_IN_24_HOURS;
        List<ForkJoinTask<Void>> taskList = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            final long nextStamp = offsetStamp + EventStat.MILLIS_IN_24_HOURS - n + i;
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
            if (i % 500 == 0) { //таким образом ограничеваем поступление событий в "будущем" до 500мс
                while (!taskList.isEmpty()) {
                    taskList.remove(0).get();
                }
            }

        }

        while (!taskList.isEmpty()) {
            taskList.remove(0).get();
        }

        assertNInLastMinuteAtTimestamp(n, es, finalStamp);
        assertNInLastHourAtTimestamp(n, es, finalStamp);
        assertNInLastDayAtTimestamp(n, es, finalStamp);

        assertNInLastMinuteAtTimestamp(n, es, finalStamp + 1);
        assertNInLastHourAtTimestamp(n, es, finalStamp + 1);
        assertNInLastDayAtTimestamp(n, es, finalStamp + 1);

    }


    @Test
    public void testConcurrentOverflow() throws ExecutionException, InterruptedException, IllegalAccessException, InvocationTargetException {
        final EventStat<Object> es = new EventStat<>();
        final long offsetStamp = getOffsetStamp(es);
        int n = 10_000;
        long finalStamp = offsetStamp + EventStat.MILLIS_IN_24_HOURS + n/2;
        List<ForkJoinTask<Void>> taskList = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            final long nextStamp = offsetStamp + EventStat.MILLIS_IN_24_HOURS - n/2 + i;
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
            if (i % 500 == 0) { //таким образом ограничеваем поступление событий в "будущем" до 500мс
                while (!taskList.isEmpty()) {
                    taskList.remove(0).get();
                }
            }
        }


        assertNInLastMinuteAtTimestamp(n, es, finalStamp);
        assertNInLastHourAtTimestamp(n, es, finalStamp);
        assertNInLastDayAtTimestamp(n, es, finalStamp);
    }

    @Test
    public void test48hr1TPS() throws IllegalAccessException, InvocationTargetException {
        final EventStat<Object> es = new EventStat<>();
        final long offsetStamp = getOffsetStamp(es);
        int n = EventStat.SECONDS_IN_24_HOURS * 2;
        long finalStamp = offsetStamp + EventStat.MILLIS_IN_24_HOURS * 2;

        for (int i = 1; i <= n; i++) {
            final long nextStamp = offsetStamp + i*1000;
            assertNotNull(insertAtStamp(new Object(), es, nextStamp > finalStamp ? finalStamp : nextStamp));
        }

        assertNInLastMinuteAtTimestamp(EventStat.SECONDS_IN_MINUTE, es, finalStamp);
        assertNInLastHourAtTimestamp(EventStat.SECONDS_IN_HOUR, es, finalStamp);
        assertNInLastDayAtTimestamp(EventStat.SECONDS_IN_24_HOURS, es, finalStamp);
    }

    @Test
    public void test48hr2TPS() throws IllegalAccessException, InvocationTargetException, ExecutionException, InterruptedException {
        final EventStat<Object> es = new EventStat<>();
        final long offsetStamp = getOffsetStamp(es);
        int n = EventStat.SECONDS_IN_24_HOURS * 2;
        long finalStamp = offsetStamp + EventStat.MILLIS_IN_24_HOURS * 2;
        List<ForkJoinTask<Void>> taskList = new ArrayList<>();
        for (int i = 0; i <= n; i++) {
            final long nextStamp = offsetStamp + i*1000;
            taskList.add(new RecursiveAction() {
                @Override
                protected void compute() {
                    try {
                        insertAtStamp(new Object(), es, nextStamp > finalStamp ? finalStamp : nextStamp);
                    } catch (InvocationTargetException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }.fork());
            taskList.add(new RecursiveAction() {
                @Override
                protected void compute() {
                    try {
                        insertAtStamp(new Object(), es, nextStamp > finalStamp ? finalStamp : nextStamp);
                    } catch (InvocationTargetException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }.fork());

            while (!taskList.isEmpty()) {
                taskList.remove(0).get();
            }
        }

        assertTrue(taskList.isEmpty());
        assertNInLastMinuteAtTimestamp(EventStat.SECONDS_IN_MINUTE*2, es, finalStamp);
        assertNInLastHourAtTimestamp(EventStat.SECONDS_IN_HOUR*2, es, finalStamp);
        assertNInLastDayAtTimestamp(n, es, finalStamp);
    }


    @Test
    public void test48hr1TPH() throws IllegalAccessException, InvocationTargetException {
        final EventStat<Object> es = new EventStat<>();
        final long offsetStamp = getOffsetStamp(es);
        int n = 48;
        long finalStamp = offsetStamp + EventStat.MILLIS_IN_24_HOURS * 2;

        for (int i = 0; i <= n; i++) {
            long nextStamp = offsetStamp + i*EventStat.MILLIS_IN_HOUR;
            assertNotNull(insertAtStamp(new Object(), es, nextStamp > finalStamp ? finalStamp : nextStamp));
        }

        assertNInLastDayAtTimestamp(n/2, es, finalStamp);
    }

    @Test
    public void test48hr2TPH() throws IllegalAccessException, InvocationTargetException, ExecutionException, InterruptedException {
        final EventStat<Object> es = new EventStat<>();
        final long offsetStamp = getOffsetStamp(es);
        int n = 48*2;
        long finalStamp = offsetStamp + EventStat.MILLIS_IN_24_HOURS * 2;
        List<ForkJoinTask<Void>> taskList = new ArrayList<>();

        for (int i = 0; i <= n; i++) {
            final long nextStamp = offsetStamp + i*EventStat.MILLIS_IN_HOUR/2;
            taskList.add(new RecursiveAction() {
                @Override
                protected void compute() {
                    try {
                        insertAtStamp(new Object(), es, nextStamp > finalStamp ? finalStamp : nextStamp);
                    } catch (InvocationTargetException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }.fork());
            taskList.add(new RecursiveAction() {
                @Override
                protected void compute() {
                    try {
                        insertAtStamp(new Object(), es, nextStamp > finalStamp ? finalStamp : nextStamp);
                    } catch (InvocationTargetException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }.fork());

            while (!taskList.isEmpty()) {
                taskList.remove(0).get();
            }
        }

        assertNInLastDayAtTimestamp(n, es, finalStamp);
    }

    @Test
    public void test48hr2TPD() throws IllegalAccessException, InvocationTargetException, ExecutionException, InterruptedException {
        final EventStat<Object> es = new EventStat<>();
        final long offsetStamp = getOffsetStamp(es);
        int n = 2;
        long finalStamp = offsetStamp + EventStat.MILLIS_IN_24_HOURS * 2;
        List<ForkJoinTask<Void>> taskList = new ArrayList<>();

        for (int i = 0; i <= n; i++) {
            final long nextStamp = offsetStamp + i*EventStat.MILLIS_IN_24_HOURS;
            taskList.add(new RecursiveAction() {
                @Override
                protected void compute() {
                    try {
                        insertAtStamp(new Object(), es, nextStamp > finalStamp ? finalStamp : nextStamp);
                    } catch (InvocationTargetException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }.fork());
            taskList.add(new RecursiveAction() {
                @Override
                protected void compute() {
                    try {
                        insertAtStamp(new Object(), es, nextStamp > finalStamp ? finalStamp : nextStamp);
                    } catch (InvocationTargetException | IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }.fork());

            while (!taskList.isEmpty()) {
                taskList.remove(0).get();
            }
        }

        assertNInLastDayAtTimestamp(n, es, finalStamp);
    }

}
