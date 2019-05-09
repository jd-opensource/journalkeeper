package com.jd.journalkeeper.core.entry;

import com.jd.journalkeeper.core.api.RaftEntryHeader;


/**
 * @author liyue25
 * Date: 2019-05-08
 */
public class EntryHeader extends RaftEntryHeader {
    private int term;
    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }
}
