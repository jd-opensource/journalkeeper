package io.journalkeeper.core.entry.reserved;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @author LiYue
 * Date: 2019-08-26
 */
public class UpdateVotersS1Entry extends ReservedEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    // TODO: delete
    private final List<URI> configOld;
    private final List<URI> configNew;
    public UpdateVotersS1Entry(List<URI> configOld, List<URI> configNew) {
        super(TYPE_UPDATE_VOTERS_S1);
        this.configOld = new ArrayList<>(configOld);
        this.configNew = new ArrayList<>(configNew);
    }

    public List<URI> getConfigOld() {
        return configOld;
    }

    public List<URI> getConfigNew() {
        return configNew;
    }
}
