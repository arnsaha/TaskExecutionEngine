package com.opentext.taskmanager;

import java.util.UUID;

public record TaskGroup(
        UUID groupUUID
) {
    public TaskGroup {
        if (groupUUID == null) {
            throw new IllegalArgumentException("All parameters must not be null");
        }
    }
}
