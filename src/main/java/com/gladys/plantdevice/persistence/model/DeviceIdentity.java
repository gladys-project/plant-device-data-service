package com.gladys.plantdevice.persistence.model;

import java.io.Serializable;
import java.util.UUID;

/** Separates mock observations from the real device's persisted state. */
public record DeviceIdentity(UUID deviceId, boolean mock) implements Serializable { }
