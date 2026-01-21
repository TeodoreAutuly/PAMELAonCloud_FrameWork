package org.openflexo.pamela.sync;

/**
 * Interface pour gérer dynamiquement une opération reçue de RabbitMQ.
 */
@FunctionalInterface
public interface RemoteOperationHandler {
    void handle(SyncOperation operation);
}