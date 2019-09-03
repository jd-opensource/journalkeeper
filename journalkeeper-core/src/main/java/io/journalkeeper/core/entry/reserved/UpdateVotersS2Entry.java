package io.journalkeeper.core.entry.reserved;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * @author LiYue
 * Date: 2019-08-26
 */
public class UpdateVotersS2Entry extends ReservedEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<URI> configNew;
    private final List<URI> configOld;
    public UpdateVotersS2Entry(List<URI> configOld, List<URI> configNew) {
        super(TYPE_UPDATE_VOTERS_S2);
        this.configNew = new ArrayList<>(configNew);
        this.configOld = new ArrayList<>(configOld);
    }

    public List<URI> getConfigNew() {
        return configNew;
    }

    public List<URI> getConfigOld() {
        return configOld;
    }
}
