/**
 * Copyright (c) 2024, Openflexo
 *
 * This file is part of Pamela-core, a component of the software infrastructure
 * developed at Openflexo.
 *
 * Openflexo is dual-licensed under the European Union Public License (EUPL, either
 * version 1.1 of the License, or any later version), which is available at
 * https://joinup.ec.europa.eu/software/page/eupl/licence-eupl
 * and the GNU General Public License (GPL, either version 3 of the License, or any
 * later version), which is available at http://www.gnu.org/licenses/gpl.html.
 */

package org.openflexo.pamela.sync;

public interface ICrdtStrategy {
    void appplyRemoteModification(SyncOperation operation,CrdtContext crdtContext); 
    void applyRemoteCreate(SyncOperation operation, CrdtContext crdtContext); 
    void applyRemoteDelete(SyncOperation operation, CrdtContext crdtContext); 

}