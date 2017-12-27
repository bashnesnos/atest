package ru.yandex.money.test.semelit;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class EventStat<T> {
    public final static int SECONDS_IN_MINUTE = 60;
    public final static int SECONDS_IN_HOUR = SECONDS_IN_MINUTE * 60;
    public final static int SECONDS_IN_24_HOURS = 24 * SECONDS_IN_HOUR;
    public final static long MILLIS_IN_24_HOURS = SECONDS_IN_24_HOURS * 1000;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private AtomicIntegerArray fullSecondSlots = new AtomicIntegerArray(new int[SECONDS_IN_24_HOURS]);
    private volatile long offsetStamp = System.currentTimeMillis();
    private volatile long lastInsertStamp = offsetStamp;

    public T insert(T event) {
        return insertAt(event, System.currentTimeMillis());
    }

    private final T insertAt(T event, long currentStamp) {
        rwLock.readLock().lock();
        try {
            if (currentStamp - lastInsertStamp > MILLIS_IN_24_HOURS) {
                //stale overflow detected
                rwLock.readLock().unlock();
                rwLock.writeLock().lock();
                try {
                    //flushing
                    if (currentStamp - lastInsertStamp > MILLIS_IN_24_HOURS) {
                        fullSecondSlots = new AtomicIntegerArray(new int[SECONDS_IN_24_HOURS]);
                        offsetStamp = currentStamp;
                    }
                    rwLock.readLock().lock();
                } finally {
                    rwLock.writeLock().unlock();
                }
            }
            if (currentStamp < lastInsertStamp - MILLIS_IN_24_HOURS) return null; //вставки за границами окна не разрешаем
            int pos = getPos(currentStamp);
            fullSecondSlots.incrementAndGet(pos);
            lastInsertStamp = currentStamp;
            return event;
        } finally {
            rwLock.readLock().unlock();
        }

    }

    private final int getPos(long currentStamp) {
        return (int) (SECONDS_IN_24_HOURS + (currentStamp - offsetStamp)/1000) % SECONDS_IN_24_HOURS;
    }

    private final int countInDuration(int durationInSeconds, long currentStamp) {
        int secondsLate = (int) (currentStamp - lastInsertStamp)/1000;
        if (secondsLate < 0) return -1; //can't read from the past

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
