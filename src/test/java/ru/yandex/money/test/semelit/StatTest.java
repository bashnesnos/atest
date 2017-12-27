package ru.yandex.money.test.semelit;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class StatTest {

    @Test
    public void testInsert() {
        EventStat<Object> es = new EventStat<>();
        Object event = new Object();
        Object insertedEvent = es.insert(event);
        assertTrue("Ожидаем то же событие после вставки, но " + insertedEvent, event == insertedEvent);
    }

    @Test
    public void testCountInLastMinute() {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertTrue("Ожидаем одно событие за последнюю минуту, но " + es.countInLastMinute(), es.countInLastMinute() == 1);
    }

    @Test
    public void testCountInLastHour() {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertTrue("Ожидаем одно событие за последний час, но " + es.countInLastHour(),es.countInLastHour() == 1);
    }

    @Test
    public void testCountInLastDay() {
        EventStat<Object> es = new EventStat<>();
        es.insert(new Object());
        assertTrue("Ожидаем одно событие за последний день, но " + es.countInLastHour(),es.countInLastDay() == 1);
    }
}
