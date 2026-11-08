package org.apache.hadoop.explorer.spark.service.engine.livy;

import java.net.URI;
import java.util.Optional;

/** Value of the Authorization header for a Livy request; empty when Livy runs without authentication. */
@FunctionalInterface
public interface LivyAuthHeaderProvider {

    LivyAuthHeaderProvider NONE = target -> Optional.empty();

    Optional<String> authorizationHeader(URI target);
}
