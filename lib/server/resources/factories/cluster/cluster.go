/*
 * Copyright 2018-2020, CS Systemes d'Information, http://www.c-s.fr
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package factory

import (
	"reflect"

	"github.com/CS-SI/SafeScale/lib/server/iaas"
	"github.com/CS-SI/SafeScale/lib/server/resources"
	"github.com/CS-SI/SafeScale/lib/server/resources/payloads/clusters"
	propertiesv1 "github.com/CS-SI/SafeScale/lib/server/resources/properties/v1"
	propertiesv2 "github.com/CS-SI/SafeScale/lib/server/resources/properties/v2"
	"github.com/CS-SI/SafeScale/lib/utils/concurrency"
	"github.com/CS-SI/SafeScale/lib/utils/data"
	"github.com/CS-SI/SafeScale/lib/utils/scerr"
)

// New creates a new instance of resources.Cluster
func New(svc iaas.Service) (_ resources.Cluster, err error) {
	if svc == nil {
		return nil, scerr.InvalidParameterError("svc", "cannot be nil")
	}
	return clusters.New(svc)
}

// Load loads metadata of a cluster and returns an instance of resources.Cluster
func Load(task concurrency.Task, svc iaas.Service, name string) (_ resources.Cluster, err error) {
	if task == nil {
		return nil, scerr.InvalidParameterError("t", "cannot be nil")
	}
	if svc == nil {
		return nil, scerr.InvalidParameterError("svc", "cannot be nil")
	}

	objc, err := clusters.New(svc)
	if err != nil {
		return nil, err
	}
	err = objc.Read(task, name)
	if err != nil {
		return nil, err
	}
	err = objc.Alter(task, func(clonable data.Clonable) error {
		ctrl, ok := clonable.(control.Controller)
		if !ok {
			return scerr.InconsistentError("'control.Controller' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		return ctrl.Bootstrap()
	})
	if err != nil {
		return nil, err
	}
	// From here, we can deal with legacy
	err = objc.upgradePropertyNodesIfNeeded(task)
	if err != nil {
		return nil, err
	}

	return objc, nil
}

// upgradePropertyNodesIfNeeded upgrade current Nodes to last Nodes (currently NodesV2)
func (objc *Cluster) upgradePropertyNodesIfNeeded(t concurrency.Task) error {
	if !objc.Properties.Lookup(ClusterProperty.NodesV2) {
		// Replace NodesV1 by NodesV2 properties
		return objc.Alter(t, func(clonable data.Clonable) error {
			return objc.Properties.Alter(ClusterProperty.NodesV2, func(clonable data.Clonable) error {
				nodesV2, ok := v.(*propertiesv2.Nodes)
				if !ok {
					return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
				}
				return objc.Properties.Alter(ClusterProperty.NodesV1, func(clonable data.Clonable) error {
					nodesV1, ok := v.(*propertiesv1.Nodes)
					if !ok {
						return scerr.InconsistentError("'*propertiesv1.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
					}
					for _, i := range nodesV1.Masters {
						nodesV2.GlobalLastIndex++

						node := &propertiesv2.Node{
							ID:          i.ID,
							NumericalID: nodesV2.GlobalLastIndex,
							Name:        i.Name,
							PrivateIP:   i.PrivateIP,
							PublicIP:    i.PublicIP,
						}
						nodesV2.Masters = append(nodesV2.Masters, node)
					}
					for _, i := range nodesV1.PrivateNodes {
						nodesV2.GlobalLastIndex++

						node := &propertiesv2.Node{
							ID:          i.ID,
							NumericalID: nodesV2.GlobalLastIndex,
							Name:        i.Name,
							PrivateIP:   i.PrivateIP,
							PublicIP:    i.PublicIP,
						}
						nodesV2.PrivateNodes = append(nodesV2.PrivateNodes, node)
					}
					nodesV2.MasterLastIndex = nodesV1.MasterLastIndex
					nodesV2.PrivateLastIndex = nodesV1.PrivateLastIndex
					nodesV1 = &propertiesv1.Nodes{}
					return nil
				})
			})
		})
	}
	return nil
}
