package ru.yandex.money.test.semelit;

import java.util.concurrent.atomic.AtomicIntegerArray;

public final class EventStat<T> {
    private final static int SECONDS_IN_MINUTE = 60;
    private final static int SECONDS_IN_HOUR = SECONDS_IN_MINUTE * 60;
    private final static int SECONDS_IN_24_HOURS = 24 * SECONDS_IN_HOUR;


    private final AtomicIntegerArray fullSecondSlots = new AtomicIntegerArray(new int[SECONDS_IN_24_HOURS]);
    private volatile long offsetStamp = System.currentTimeMillis();

    public T insert(T event) {
        return insertToLastMinute(event);
    }

    private final T insertToLastMinute(T event) {
        int pos = getPos();
        fullSecondSlots.incrementAndGet(pos);
        return event;
    }

    private final int getPos() {
        long currentSecond = System.currentTimeMillis();
        return (int) ((currentSecond - offsetStamp)/1000) % SECONDS_IN_24_HOURS;
    }

    private final int countInDuration(int durationInSeconds) {
        int result = 0;
        for (int i = getPos(); i % SECONDS_IN_24_HOURS < durationInSeconds; i++) {
            result += fullSecondSlots.get(i);
        }
        return result;
    }

    public int countInLastMinute() {
        return countInDuration(SECONDS_IN_MINUTE);
    }

    public int countInLastHour() {
        return countInDuration(SECONDS_IN_HOUR);
    }

    public int countInLastDay() {
        int result = 0;
        for (int i = 0; i < SECONDS_IN_24_HOURS; i++) {
            result += fullSecondSlots.get(i);
        }
        return result;
    }
}
