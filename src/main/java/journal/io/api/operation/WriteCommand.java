package journal.io.api.operation;

import journal.io.api.Location;

/**
 * Created by Andrew on 12.04.2017.
 */
public class WriteCommand {

    private final Location location;
    private final boolean sync;
    private volatile byte[] data;

    WriteCommand(Location location, byte[] data, boolean sync) {
        this.location = location;
        this.data = data;
        this.sync = sync;
    }

    public Location getLocation() {
        return location;
    }

    byte[] getData() {
        return data;
    }

    boolean isSync() {
        return sync;
    }
}
