package journal.io.api.operation;

import journal.io.api.Journal;
import journal.io.api.Location;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Andrew on 12.04.2017.
 */
public class Redo implements Iterable<Location> {

    private Journal journal;
    private final Location start;

    public Redo(Journal journal, Location start) {
        this.journal = journal;
        this.start = start;
    }

    public Iterator<Location> iterator() {
        return new Iterator<Location>() {
            private Location current = null;
            private Location next = start;

            public boolean hasNext() {
                return next != null;
            }

            public Location next() {
                if (next != null) {
                    try {
                        current = next;
                        next = journal.goToNextLocation(current, Location.USER_RECORD_TYPE, true);
                        // TODO: reading the next next location at this point
                        // (*before* the related hasNext call) is not
                        // really correct.
                        return current;
                    } catch (IOException ex) {
                        throw new IllegalStateException(ex.getMessage(), ex);
                    }
                } else {
                    throw new NoSuchElementException();
                }
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
