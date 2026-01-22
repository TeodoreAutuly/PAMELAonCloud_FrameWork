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

import java.util.Map;
import org.openflexo.pamela.factory.PamelaModelFactory;
import java.util.logging.Logger;

public class CrdtContext {
public final ObjectIdentityManager identityManager;
    public final PropertyStateManager propertyStateManager;
    public final PamelaModelFactory modelFactory;
    public final SyncValueSerializer valueSerializer;
    public final Logger logger;
    public final Map<String, Boolean> createdObjects;

    public CrdtContext(
        ObjectIdentityManager identityManager,
        PropertyStateManager propertyStateManager,
        PamelaModelFactory modelFactory,
        SyncValueSerializer valueSerializer,
        Logger logger,
        Map<String, Boolean> createdObjects
    ) {
        this.identityManager = identityManager;
        this.propertyStateManager = propertyStateManager;
        this.modelFactory = modelFactory;
        this.valueSerializer = valueSerializer;
        this.logger = logger;
        this.createdObjects = createdObjects;
    }
}