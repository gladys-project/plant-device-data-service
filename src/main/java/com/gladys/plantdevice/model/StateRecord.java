package com.gladys.plantdevice.model;

import java.time.Instant;

public record StateRecord(String code, Instant lastHappened) {
}
