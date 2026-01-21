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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openflexo.pamela.AccessibleProxyObject;
import org.openflexo.pamela.sync.SyncOperation.OperationType;

public class PropertyStateManager{
    private final Map<String,Map<String,SyncOperation>> mapCrdt; 
    private final ObjectIdentityManager objectIdentityManager; 
    public PropertyStateManager(ObjectIdentityManager objectIdentityManager){
        this.mapCrdt =new HashMap<>(); 
        this.objectIdentityManager = objectIdentityManager; 
    }


    public Map<String,Map<String,SyncOperation>> getMapCrdt(){
        return this.mapCrdt; 
    }

    public void storeIntoMap(SyncOperation operation){
        if(operation.getOperationType().equals(OperationType.SET)||operation.getOperationType().equals(OperationType.REINDEX))
         mapCrdt.computeIfAbsent(operation.getObjectId(), id -> new HashMap<>()).put(operation.getPropertyIdentifier(), operation);
        if(operation.getOperationType().equals(OperationType.DELETE))
            recursiveDelete(operation,operation.getObjectId());
    }

    private void recursiveDelete(SyncOperation operation, String initObjectId){
        AccessibleProxyObject object = (AccessibleProxyObject)objectIdentityManager.getObject(initObjectId);
        for(String property : mapCrdt.get(initObjectId).keySet()){
            mapCrdt.get(initObjectId).put(property, operation);
        }   
        List<? extends AccessibleProxyObject> referencedObjects = object.getReferencedObjects();
        for(AccessibleProxyObject referencedObject : referencedObjects){
            String newObjectId = objectIdentityManager.getObjectId(referencedObject);
            recursiveDelete(operation, newObjectId);
        }
    }
}
