package com.jerolba.carpet.labs;

import java.util.List;

public interface CarpetOutput {

    /**
     * Returns the list of file paths that were created during writing.
     * This list is populated after the writer has been closed.
     *
     * @return List of file paths created
     */
    List<String> getCreatedFiles();

}
