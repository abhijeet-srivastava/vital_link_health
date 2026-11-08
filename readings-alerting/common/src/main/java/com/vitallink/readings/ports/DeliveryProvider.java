package com.vitallink.readings.ports;

import com.vitallink.readings.domain.PendingNotification;

/**
 * A push or SMS provider. The provider deduplicates on {@code notificationId} (idempotency key, guard 6), so a
 * repeated call after a crash between the call and the SENT update delivers nothing new. The provider looks up the
 * coach's device or number itself: no contact details pass through this service.
 *
 * @throws com.vitallink.readings.domain.DeliveryException on rejection or failure
 */
public interface DeliveryProvider {

    String name();

    void deliver(PendingNotification notification);
}
