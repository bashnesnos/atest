package ru.yandex.money.test.semelit;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class EventStat<T> {
    public final static int SECONDS_IN_MINUTE = 60;
    public final static int SECONDS_IN_HOUR = SECONDS_IN_MINUTE * 60;
    public final static int SECONDS_IN_24_HOURS = 24 * SECONDS_IN_HOUR;
    public final static long MILLIS_IN_MINUTE = SECONDS_IN_MINUTE * 1000;
    public final static long MILLIS_IN_HOUR = SECONDS_IN_HOUR * 1000;
    public final static long MILLIS_IN_24_HOURS = SECONDS_IN_24_HOURS * 1000;

    private final static int SLOTS = SECONDS_IN_24_HOURS + 1; //на один больше для полных 24 часов

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private AtomicIntegerArray fullSecondSlots = new AtomicIntegerArray(new int[SLOTS]);
    private volatile long offsetStamp = System.currentTimeMillis();
    private volatile long lastInsertStamp = offsetStamp;

    public T insert(T event) {
        return insertAt(event, System.currentTimeMillis());
    }

    private final T insertAt(T event, long currentStamp) {
        rwLock.readLock().lock();
        try {
            final int pos = getPos(currentStamp);
            if (currentStamp - lastInsertStamp > 1000) {
                //lag detected
                rwLock.readLock().unlock();
                rwLock.writeLock().lock();
                try {
                    //flushing
                    long lag = currentStamp - lastInsertStamp;
                    if (lag > MILLIS_IN_24_HOURS) {
                        fullSecondSlots = new AtomicIntegerArray(new int[SLOTS]);
                        offsetStamp = currentStamp;
                    } else if (lag > 1000) {
                        int end = SLOTS + pos + 1;
                        int start = end - ((int) (lag/1000));
                        if (start < end) { //protection from overflow
                            for (int i = start; i < end; i++) {
                                fullSecondSlots.set(i % SLOTS, 0);
                            }
                        }
                    }
                    lastInsertStamp = currentStamp;
                    rwLock.readLock().lock();
                } finally {
                    rwLock.writeLock().unlock();
                }
            }
            if (currentStamp < lastInsertStamp - MILLIS_IN_24_HOURS) return null; //вставки за границами окна не разрешаем
            fullSecondSlots.incrementAndGet(pos);
            fullSecondSlots.set((pos + 1) % SLOTS, 0);
            lastInsertStamp = currentStamp;
            return event;
        } finally {
            rwLock.readLock().unlock();
        }

    }

    private final int getPos(long currentStamp) {
        return (int) (currentStamp - offsetStamp)/1000 % SLOTS;
    }

    private final int countInDuration(int durationInSeconds, long currentStamp) {
        int secondsLate = (int) (currentStamp - lastInsertStamp)/1000;
        if (secondsLate < 0) return -1; //can't read from the past

        int result = 0;
        int end = SLOTS + getPos(currentStamp) + 1;
        int start = end + secondsLate - durationInSeconds;
        if (start < end) { //protection from reading stale
            for (int i = start; i < end; i++) {
                result += fullSecondSlots.get(i % SLOTS);
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
