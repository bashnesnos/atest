package ru.yandex.money.test.semelit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * Реализация объекта для учёта однотипных событий на основе ring-buffer
 * Алгоритм строится на том, что время подделать трудно и события не могут приходить далеко в будущем (более секунды)
 * В противном случае, корректная работа не гарантируется
 * Хранит кол-во событий с разрешением в 1 секунду за последние 24 часа
 * Позволяет подсчитывать кол-во событий за последние 60 секунд, 60 минут либо 24 часа
 * Вставки и подсчёт за пределами 24 часов запрещены
 *
 * Вставка события осуществляется в худшем случае за O(n), где n <= 86400
 * Очистка слотов за границами окна происходит при вставке
 * В среднем случае равномерного кол-ва событий (где разница между событиями в пределах 1-2 секунд) вставка за O(1)
 * Подсчёт кол-ва за O(n), где n <= 86400
 *
 * Память в худшем случае 2*O(m), где m = 86401, т.е. хранится массив счётчиков для каждой секунды в сутках
 * В среднем, для случаев без длительных простоев (менее суток) это O(m), где m = 86401
 *
 * Потокобезопасен
 * В случае равномерной нагрузки без провалов (в пределах 1 секунды) можно считать lock-free
 * При случае с провалами происходит блокировка всех вставок на время устранения провалов
 *
 * @param <T> Любой тип событий
 */
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
    private AtomicInteger clearMark = new AtomicInteger(0);

    private volatile long lastInsertStamp = offsetStamp;

    public T insert(T event) {
        return insertAt(event, System.currentTimeMillis());
    }

    private final T insertAt(T event, long currentStamp) {
        rwLock.readLock().lock();
        try {
            final int pos = getPos(currentStamp);
            if (currentStamp - lastInsertStamp > 1000) {
                //обнаружен лаг
                //необходима очистка слотов, в которых скорее всего устаревшие события
                //чистим основательно и блокируем все вставки на длительность очистки
                //это плохо для случаев с резкими всплесками и полным отсутствием событий между такими всплесками

                rwLock.readLock().unlock();
                rwLock.writeLock().lock();
                try {
                    long lag = currentStamp - lastInsertStamp;
                    if (lag > MILLIS_IN_24_HOURS) {
                        //в случае такого простоя проще создать новый массив
                        fullSecondSlots = new AtomicIntegerArray(new int[SLOTS]);
                    } else if (lag > 1000) {
                        //это означает, что мы ничего не вставляли, а соответственно и не очищали слоты
                        //поэтому включаем в очистку текущую позицию, чтобы не дописывать в устаревшие данные
                        int end = SLOTS + pos + 1;
                        int start = end - ((int) (lag/1000));
                        for (int i = start; i < end; i++) {
                            fullSecondSlots.set(i % SLOTS, 0);
                        }
                    }
                    lastInsertStamp = currentStamp;
                    rwLock.readLock().lock();
                } finally {
                    rwLock.writeLock().unlock();
                }
            }
            if (currentStamp < lastInsertStamp - MILLIS_IN_24_HOURS) return null; //вставки за границами окна не разрешаем
            if (clearMark.compareAndSet(pos, (pos + 1) % SLOTS)) {
                //для равномерной нагрузки очистка происходит в момент переключения текущего слота
                fullSecondSlots.set(clearMark.get(), 0); //основываясь на предположении, что в будущем в пределах одной секунды никто не добавляет события
            }
            fullSecondSlots.incrementAndGet(pos);
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
        if (secondsLate < 0) return -1; //не позволяем читать из прошлого

        int result = 0;
        int end = SLOTS + getPos(currentStamp) + 1;
        int start = end + secondsLate - durationInSeconds;
        if (start < end) { //в случае запоздания большего чем запрошенный период нет смысла считать
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
