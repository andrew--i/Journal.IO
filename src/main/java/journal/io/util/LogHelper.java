/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package journal.io.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author Sergio Bossa
 */
public class LogHelper {

	private static final Logger LOG = LoggerFactory.getLogger(LogHelper.class.getName());

	public static void warn(String message, Object... args) {
		if (LOG.isWarnEnabled()) {
			LOG.warn(String.format(message, args));
		}
	}

	public static void warn(Throwable e, String message) {
		if (LOG.isWarnEnabled()) {
			LOG.warn(message, e);
		}
	}

	public static void warn(Throwable e, String message, Object... args) {
		if (LOG.isWarnEnabled()) {
			LOG.warn(String.format(message, args), e);
		}
	}

	public static void error(String message, Object... args) {
		if (LOG.isErrorEnabled()) {
			LOG.error(String.format(message, args));
		}
	}

	public static void error(Throwable e, String message) {
		if (LOG.isErrorEnabled()) {
			LOG.error(message, e);
		}
	}

	public static void error(Throwable e, String message, Object... args) {
		if (LOG.isErrorEnabled()) {
			LOG.error(String.format(message, args), e);
		}
	}

}