package org.lyb.splitstream.events;

import java.util.Random;
import java.util.function.Supplier;
import net.datafaker.Faker;
import org.lyb.splitstream.event.Event;

/** A supplier that produces Events. */
public class EventSupplier implements Supplier<Event> {
    private final Random random = new Random();
    private final Faker faker = new Faker();
    private int id = 0;

    @Override
    public Event get() {
        String ibanAccountNumber = faker.finance().iban();
        return new Event(
                id++,
                ibanAccountNumber,
                Event.Priority.values()[random.nextInt(Event.Priority.values().length)]);
    }
}
