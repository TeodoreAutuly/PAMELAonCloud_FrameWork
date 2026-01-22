/**
 * Copyright (c) 2026, Openflexo
 * 
 * This file is part of Pamela-core, a component of the software infrastructure 
 * developed at Openflexo.
 * 
 * Openflexo is dual-licensed under the European Union Public License (EUPL, either 
 * version 1.1 of the License, or any later version ), which is available at 
 * https://joinup.ec.europa.eu/software/page/eupl/licence-eupl
 * and the GNU General Public License (GPL, either version 3 of the License, or any 
 * later version), which is available at http://www.gnu.org/licenses/gpl.html .
 */
package org.openflexo.pamela.sync;

/**
 * Interface pour gérer dynamiquement une opération reçue de RabbitMQ.
 */
@FunctionalInterface
public interface RemoteOperationHandler {
    void handle(SyncOperation operation);
}