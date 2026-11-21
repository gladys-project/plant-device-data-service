package com.gladys.plantdevice.persistence.model;

import java.io.Serializable;
import java.util.UUID;

import lombok.Builder;

/** Separates mock observations from the real device's persisted state. */
@Builder
public record DeviceIdentity(UUID deviceId, boolean mock) implements Serializable { }
