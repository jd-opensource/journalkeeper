package io.journalkeeper.core.state;

import java.net.URI;
import java.util.List;
/**
 * Voter change listener
 **/
public interface ConfigStateChangeListener {
    void onNewConfig(List<URI> voters);
}
