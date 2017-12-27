package ru.yandex.money.test.semelit;

import java.util.concurrent.atomic.AtomicIntegerArray;

public final class EventStat<T> {
    public final static int SECONDS_IN_MINUTE = 60;
    public final static int SECONDS_IN_HOUR = SECONDS_IN_MINUTE * 60;
    public final static int SECONDS_IN_24_HOURS = 24 * SECONDS_IN_HOUR;


    private final AtomicIntegerArray fullSecondSlots = new AtomicIntegerArray(new int[SECONDS_IN_24_HOURS]);
    private final long offsetStamp = System.currentTimeMillis();
    private volatile long lastInsertStamp = System.currentTimeMillis();

    public T insert(T event) {
        return insertAt(event, System.currentTimeMillis());
    }

    private final T insertAt(T event, long currentStamp) {
        lastInsertStamp = currentStamp;
        fullSecondSlots.incrementAndGet(getPos(currentStamp));
        return event;
    }

    private final int getPos(long currentStamp) {
        return (int) (currentStamp - offsetStamp)/1000 % SECONDS_IN_24_HOURS;
    }

    private final int countInDuration(int durationInSeconds, long currentStamp) {
        int secondsLate = (int) (currentStamp - lastInsertStamp)/1000;
        int result = 0;
        int end = SECONDS_IN_24_HOURS + getPos(currentStamp) + 1;
        int start = end + secondsLate - durationInSeconds;
        if (start < end) { //protection from reading stale
            for (int i = start; i < end; i++) {
                result += fullSecondSlots.get(i % SECONDS_IN_24_HOURS);
            }
        }
        return result;
    }

    public int countInLastMinute() {
        return countInDuration(SECONDS_IN_MINUTE, System.currentTimeMillis());
    }

    public int countInLastHour() {
        return countInDuration(SECONDS_IN_HOUR, System.currentTimeMillis());
    }

    public int countInLastDay() {
        return countInDuration(SECONDS_IN_24_HOURS, System.currentTimeMillis());
    }
}
