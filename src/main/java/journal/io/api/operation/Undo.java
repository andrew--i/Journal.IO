package journal.io.api.operation;

import journal.io.api.Location;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Andrew on 12.04.2017.
 */
public class Undo implements Iterable<Location> {

    private final Object[] stack;
    private final int start;

    public Undo(Iterable<Location> redo) {
        // Object arrays of 12 are about the size of a cache-line (64 bytes)
        // or two, depending on the oops-size.
        Object[] stack = new Object[12];
        // the last element of the arrays refer to the next "fat node."
        // the last element of the last node is null as an end-mark
        int pointer = 10;
        Iterator<Location> itr = redo.iterator();
        while (itr.hasNext()) {
            Location location = itr.next();
            stack[pointer] = location;
            if (pointer == 0) {
                Object[] tmp = new Object[12];
                tmp[11] = stack;
                stack = tmp;
                pointer = 10;
            } else {
                pointer--;
            }
        }
        this.start = pointer + 1; // +1 to go back to last write
        this.stack = stack;
    }

    @Override
    public Iterator<Location> iterator() {
        return new Iterator<Location>() {
            private int pointer = start;
            private Object[] ref = stack;
            private Location current;

            @Override
            public boolean hasNext() {
                return ref[pointer] != null;
            }

            @Override
            public Location next() {
                Object next = ref[pointer];
                if (!(ref[pointer] instanceof Location)) {
                    ref = (Object[]) ref[pointer];
                    if (ref == null) {
                        throw new NoSuchElementException();
                    }
                    pointer = 0;
                    return next();
                }
                pointer++;
                return current = (Location) next;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
