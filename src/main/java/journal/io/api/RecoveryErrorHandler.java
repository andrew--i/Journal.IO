package journal.io.api;

import java.io.IOException;
import java.util.List;

import static journal.io.util.LogHelper.error;
import static journal.io.util.LogHelper.warn;

/**
 * Interface to implement for dealing with checksum errors during journal
 * recovery.
 *
 * @author Sergio Bossa
 */
public interface RecoveryErrorHandler {

	/**
	 * On recovery error, aborts the recovery process, preventing the journal to
	 * be opened.
	 * <p>
	 * This is the strictest but safest choice.
	 */
	public static RecoveryErrorHandler ABORT = new AbortOnError();


	/**
	 * Invoked by the journal during its recovery process in case of a checksum
	 * error for the given locations.
	 * <p>
	 * Please note this method will be invoked once for any batch containing failed locations.
	 */
	void onError(Journal journal, List<Location> locations) throws IOException;

	public static class AbortOnError implements RecoveryErrorHandler {

		@Override
		public void onError(Journal journal, List<Location> locations) throws IOException {
			for (Location location : locations) {
				error("Bad checksum for location: " + location);
			}
			throw new IOException("Aborting recovery process!");
		}
	}
}
