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

package clusters

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/CS-SI/SafeScale/lib/server/iaas"
	"github.com/CS-SI/SafeScale/lib/server/resources/abstracts"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clustercomplexity"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clusterproperty"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/installmethod"
	"github.com/CS-SI/SafeScale/lib/server/resources/payloads"
	propertiesv1 "github.com/CS-SI/SafeScale/lib/server/resources/properties/v1"
	propertiesv2 "github.com/CS-SI/SafeScale/lib/server/resources/properties/v2"
	"github.com/CS-SI/SafeScale/lib/utils/concurrency"
	"github.com/CS-SI/SafeScale/lib/utils/data"
	"github.com/CS-SI/SafeScale/lib/utils/retry"
	"github.com/CS-SI/SafeScale/lib/utils/scerr"
	"github.com/CS-SI/SafeScale/lib/utils/serialize"
	"github.com/CS-SI/SafeScale/lib/utils/temporal"
)

const (
	// Path is the path to use to reach Cluster Definitions/Metadata
	clustersFolderName = "clusters"
)

// Cluster is the cluster definition stored in ObjectStorage
type Cluster struct {
	*payloads.Core
	installMethods      map[uint8]installmethod.Enum
	lastStateCollection time.Time
}

// Browse walks through cluster folder and executes a callback for each entry
func (c *Cluster) Browse(task concurrency.Task, callback func([]byte) error) error {
	// Note: Allows to browse with an oc == nil by design
	if task == nil {
		return scerr.InvalidParameterError("task", "cannot be nil")
	}
	if callback == nil {
		return scerr.InvalidParameterError("callback", "cannot be nil")
	}

	return c.core.Browse(task, callback)
}

// Create creates a new cluster and save its metadata
func (c *Cluster) Create(task concurrency.Task, req resources.ClusterRequest) (err error) {
	if c == nil {
		return scerr.InvalidInstanceError()
	}
	if task == nil {
		return scerr.InvalidParameterError("task", "cannot be nil")
	}

	tracer := concurrency.NewTracer(nil, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	// VPL: For now, always disable addition of feature proxycache-client
	props, err := c.Properties(task)
	if err != nil {
		return err
	}
	err = props.Alter(clusterproperty.FeaturesV1, func(clonable data.Clonable) error {
		featuresV1, ok := clonable.(*propertiesv1.Features)
		if !ok {
			return scerr.InconsistentError("'*propertiesv1.Features' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		featuresV1.Disabled["proxycache"] = struct{}{}
		return nil
	})
	if err != nil {
		logrus.Errorf("failed to disable feature 'proxycache': %v", err)
		return err
	}
	// ENDVPL

	// Creates cluster controller...
	ctrl, err := clusters.NewController(task, svc, req)
	if err != nil {
		return err
	}
	// ... and prepares it for duty
	err = ctrl.Bootstrap()
	if err != nil {
		return err
	}
	err = c.Carry(task, ctrl)
	if err != nil {
		return err
	}

	err = c.Alter(task, func(clonable data.Clonable, props *serialize.JSONProperties) error {
		inErr := props.Alter(Property.DefaultsV2, func(clonable data.Clonable) error {
			defaultsV2, ok := clonable.(*propertiesv2.Defaults)
			if !ok {
				return scerr.InconsistentError("'*propertiesv2.Defaults' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			defaultsV2.GatewaySizing = srvutils.FromProtocolHostSizing(*req.GatewaysDef.Sizing)
			defaultsV2.MasterSizing = srvutils.FromProtocolHostSizing(*req.MastersDef.Sizing)
			defaultsV2.NodeSizing = srvutils.FromProtocolHostSizing(*req.NodesDef.Sizing)
			// FIXME: how to recover image ID from construct() ?
			// defaultsV2.Image = imageID
			return nil
		})
		if inErr != nil {
			return inErr
		}

		inErr = props.Alter(Property.StateV1, func(clonable data.Clonable) error {
			stateV1, ok := clonable.(*propertiesv1.State)
			if !ok {
				return scerr.InconsistentError("'*propertiesv1.State' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			stateV1.State = ClusterState.Creating
			return nil
		})
		if inErr != nil {
			return inErr
		}

		return c.Properties.Alter(Property.CompositeV1, func(clonable data.Clonable) error {
			compositeV1, ok := clonable.(*propertiesv1.Composite)
			if !ok {
				return scerr.InconsistentError("'*propertiesv1.Composite' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			compositeV1.Tenants = []string{req.Tenant}
			return nil
		})
	})
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			derr := c.Delete(task)
			if derr != nil {
				logrus.Errorf("after failure, cleanup failed to delete cluster metadata")
			}
		}
	}()

	err = c.Alter(task, func(clonable data.Clonable) error {
		// Launch infrastructure construction
		err = ctrl.construct()
		if err != nil {
			return err
		}

		// Updates metadata after cluster created successfully
		objn, inErr := LoadNetwork(c.Service(), req.NetworkID)
		if inErr != nil {
			return inErr
		}
		var (
			objpgw, objsgw *Host
		)
		inErr = objn.Inspect(task, func(clonable data.Clonable) error {
			rn, ok := clonable.(*abstracts.Network)
			if !ok {
				return scerr.InconsistentError("'*abstracts.Network' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			var hostErr error
			objpgw, hostErr = LoadHost(c.Service(), rn.GatewayID)
			if hostErr != nil {
				return hostErr
			}
			if rn.SecondaryGatewayID != "" {
				objpgw, hostErr = LoadHost(c.Service(), rn.GatewayID)
				if hostErr != nil {
					return hostErr
				}
			}
			return nil
		})
		objpgw, inErr = LoadHost(c.Service(), objn)
		return c.Properties.Alter(Property.NetworkV2, func(clonable data.Clonable) error {
			networkV2, ok := clonable.(*propertiesv2.Network)
			if !ok {
				return scerr.InconsistentError("'*propertiesv2.Network' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			networkV2.NetworkID = req.NetworkID
			networkV2.CIDR = req.CIDR
			networkV2.GatewayID = objpgw.ID(task)
			networkV2.GatewayIP = objpgw.PrivateIP(task)
			if !gwFailoverDisabled {
				networkV2.SecondaryGatewayID = secondaryGateway.ID
				networkV2.SecondaryGatewayIP = secondaryGateway.GetPrivateIP()
				networkV2.DefaultRouteIP = network.VirtualIp.PrivateIp
				// VPL: no public IP on VIP yet...
				// networkV2.EndpointIP = network.VirtualIp.PublicIp
				networkV2.EndpointIP = primaryGateway.GetPublicIP()
				networkV2.PrimaryPublicIP = primaryGateway.GetPublicIP()
				networkV2.SecondaryPublicIP = secondaryGateway.GetPublicIP()
			} else {
				networkV2.DefaultRouteIP = primaryGateway.GetPrivateIP()
				networkV2.EndpointIP = primaryGateway.GetPublicIP()
				networkV2.PrimaryPublicIP = networkV2.EndpointIP
			}
			return nil
		})
	})
	if err != nil {
		return err
	}

}

// Identity returns the identity of the cluster
func (c *Cluster) Identity(task concurrency.Task) (identity resources.ClusterIdentity, err error) {
	if oc == nil {
		return 0, scerr.InvalidInstanceError()
	}
	if task == nil {
		return 0, scerr.InvalidParameterError("task", "cannot be nil")
	}

	err = c.Inspect(task, func(clonable data.Clonable) error {
		rc, ok := clonable.(cluster.Controller)
		if ok != nil {
			return scerr.InconsistentError("'cluster.Controller' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		identity = rc.Identity(task)
		return nil
	})
}

// Flavor returns the flavor of the cluster
//
// satisfies interface cluster.Controller
func (oc *Cluster) Flavor(task concurrency.Task) (flavor ClusterFlavor.Enum, err error) {
	if oc == nil {
		return 0, scerr.InvalidInstanceError()
	}
	if task == nil {
		return 0, scerr.InvalidParameterError("task", "cannot be nil")
	}

	tracer := concurrency.NewTracer(nil, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	err = oc.Inspect(task, func(clonable data.Clonable) error {
		ctrl, ok := clonable.(cluster.Controller)
		if !ok {
			return scerr.InconsistentError("'cluster.Controller' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		var inErr error
		flavor, inErr = ctrl.Flavor()
		return inErr
	})
	if err != nil {
		return 0, err
	}
	return flavor, nil
}

// Complexity returns the complexity of the cluster
// satisfies interface cluster.Controller
func (oc *Cluster) Complexity(task concurrency.Task) (complexity clustercomplexity.Enum, err error) {
	if oc == nil {
		return 0, scerr.InvalidInstanceError()
	}
	if task == nil {
		return 0, scerr.InvalidParameterError("task", "cannot be nil")
	}

	tracer := concurrency.NewTracer(nil, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	err = oc.Inspect(task, func(clonable data.Clonable) error {
		controller, ok := clonable.(cluster.Controller)
		if !ok {
			return scerr.InconsistentError("'cluster.Controller' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		var inErr error
		complexity, inErr = controller.Complexity()
		return inErr
	})
	if err != nil {
		return 0, err
	}
	return complexity, nil
}

// AdminPassword returns the password of the cluster admin account
// satisfies interface cluster.Controller
func (oc *Cluster) AdminPassword(task concurrency.Task) (adminPassword string, err error) {
	if oc == nil {
		return "", scerr.InvalidInstanceError()
	}
	if task == nil {
		return "", scerr.InvalidParameterError("task", "cannot be nil")
	}

	tracer := concurrency.NewTracer(nil, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	err = oc.Inspect(task, func(clonable data.Clonable) error {
		controller, ok := clonable.(cluster.Controller)
		if !ok {
			return scerr.InconsistentError("'cluster.Controller' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		var inErr error
		adminPassword, inErr = controller.AdminPassword()
		return inErr
	})
	return adminPassword, err
}

// KeyPair returns the key pair used in the cluster
// satisfies interface cluster.Controller
func (oc *Cluster) KeyPair(task concurrency.Task) (keyPair abstracts.KeyPair, err error) {
	if oc == nil {
		return keyPair, scerr.InvalidInstanceError()
	}
	if task == nil {
		return keyPair, scerr.InvalidParameterError("task", "cannot be nil")
	}

	tracer := concurrency.NewTracer(nil, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	err = mc.Inspect(task, func(clonable data.Clonable) error {
		controller, ok := clonable.(cluster.Controller)
		if !ok {
			return scerr.InconsistentError("'cluster.Controller' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		var inErr error
		keyPair, inErr = controller.KeyPair()
		return inErr
	})
	return keyPair, err
}

// NetworkConfig returns network configuration of the cluster
// satisfies interface cluster.Controller
func (c *Cluster) NetworkConfig(task concurrency.Task) (config *propertiesv1.Network, err error) {
	if c == nil {
		return cfg, scerr.InvalidInstanceError()
	}
	if task == nil {
		return cfg, scerr.InvalidParameterError("task", "cannot be nil")
	}

	tracer := concurrency.NewTracer(task, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	if c.Properties.Lookup(Property.NetworkV2) {
		_ = c.Properties.Inspect(Property.NetworkV2, func(clonable data.Clonable) error {
			networkV2, ok := clonable.(*propertiesv2.Network)
			if !ok {
				return scerr.InconsistentError("'*propertiesv2.Network' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			config = networkV2
			return nil
		})
	} else {
		err = c.Alter(func(clonable data.Clonable) error {
			err = c.Properties.Inspect(Property.NetworkV1, func(v data.Clonable) error {
				networkV1 := v.(*propertiesv1.Network)
				config = &propertiesv2.Network{
					NetworkID:      networkV1.NetworkID,
					CIDR:           networkV1.CIDR,
					GatewayID:      networkV1.GatewayID,
					GatewayIP:      networkV1.GatewayIP,
					DefaultRouteIP: networkV1.GatewayIP,
					EndpointIP:     networkV1.PublicIP,
				}
				return nil
			})
			if err != nil {
				return err
			}
			return c.Properties.Alter(Property.NetworkV1, func(clonable data.Clonable) error {
				networkV2, ok := clonable.(*propertiesv2.Network)
				if !ok {
					return scerr.InconsistentError("'*propertiesv2.Network' expected, '%s' provided", reflect.TypeOf(clonable).String())
				}
				_ = networkV2.Replace(config)
				return nil
			})
		})
		if err != nil {
			return nil, err
		}
	}

	return config, nil
}

// Properties returns the extension of the cluster
//
// satisfies interface cluster.Controller
func (objc *Cluster) Properties(task concurrency.Task) (props *serialize.JSONProperties, err error) {
	if objc == nil {
		return nil, scerr.InvalidInstanceError()
	}
	if task == nil {
		return nil, scerr.InvalidParameterError("task", "cannot be nil")
	}

	tracer := concurrency.NewTracer(nil, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	objc.lock.RLock(task)
	defer objc.lock.RUnlock(task)
	return nil, objc.properties
}

// Start starts the cluster
// satisfies interface cluster.cluster.Controller
func (c *Cluster) Start(task concurrency.Task) (err error) {
	if c == nil {
		return scerr.InvalidInstanceError()
	}
	defer scerr.OnPanic(&err)()

	if task == nil {
		return scerr.InvalidParameterError("task", "cannot be nil")
	}

	tracer := concurrency.NewTracer(nil, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	var prevState ClusterState
	prevState, err = c.State(task)
	if err != nil {
		return err
	}

	// If the cluster is in state Nominal or Degraded, do nothing
	if prevState == ClusterState.Nominal || prevState == ClusterState.Degraded {
		return nil
	}

	// If the cluster is in state Starting, wait for it to finish its start procedure
	if prevState == ClusterState.Starting {
		err = retry.WhileUnsuccessfulDelay5Seconds(
			func() error {
				state, inErr := c.State(task)
				if inErr != nil {
					return inErr
				}
				if state == ClusterState.Nominal || state == ClusterState.Degraded {
					return nil
				}
				return fmt.Errorf("current state of cluster is '%s'", state.String())
			},
			5*time.Minute, // FIXME: static timeout
		)
		if err != nil {
			switch err.(type) {
			case retry.ErrTimeout:
				err := scerr.Wrap(err, "timeout waiting cluster to become started")
			}
			return err
		}
		return nil
	}

	if prevState != ClusterState.Stopped {
		return scerr.NotAvailableError("failed to start cluster because of it's current state: %s", prevState.String())
	}

	// First mark cluster to be in state Starting
	err = c.Alter(task, func(clonable data.Clonable) error {
		return c.Properties.Alter(Property.StateV1, func(clonable data.Clonable) error {
			stateV1, ok := clonable.(*clusterpropertiesv1.State)
			if !ok {
				return scerr.InconsistentError("'*clusterpropertiesv1.State' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			stateV1.State = ClusterState.Starting
		})
	})
	if err != nil {
		return err
	}

	// Then start it and mark it as STARTED on success
	return c.Alter(task, func(clonable data.Clonable) error {
		var (
			nodes                         []*propertiesv2.Node
			masters                       []*propertiesv2.Node
			gatewayID, secondaryGatewayID string
		)
		err = c.Properties.Inspect(Property.NodesV2, func(v interface{}) error {
			nodesV2 := v.(*propertiesv2.Nodes)
			masters = nodesV2.Masters
			nodes = nodesV2.PrivateNodes
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to get list of hosts: %v", err)
		}
		if c.Properties.Lookup(Property.NetworkV2) {
			err = c.Properties.Inspect(Property.NetworkV2, func(v interface{}) error {
				networkV2 := v.(*propertiesv2.Network)
				gatewayID = networkV2.GatewayID
				secondaryGatewayID = networkV2.SecondaryGatewayID
				return nil
			})
		} else {
			err = c.Properties.Inspect(Property.NetworkV1, func(v interface{}) error {
				gatewayID = v.(*propertiesv1.Network).GatewayID
				return nil
			})
		}
		if err != nil {
			return err
		}

		// FIXME introduce status

		// Start gateway(s)
		taskGroup, err := concurrency.NewTaskGroup(task)
		if err != nil {
			return err
		}
		_, err = taskGroup.Start(c.asyncStartHost, gatewayID)
		if err != nil {
			return err
		}
		if secondaryGatewayID != "" {
			_, err = taskGroup.Start(c.asyncStartHost, secondaryGatewayID)
			if err != nil {
				return err
			}
		}
		// Start masters
		for _, n := range masters {
			_, err = taskGroup.Start(c.asyncStartHost, n.ID)
			if err != nil {
				return err
			}
		}
		// Start nodes
		for _, n := range nodes {
			_, err = taskGroup.Start(c.asyncStartHost, n.ID)
			if err != nil {
				return err
			}
		}
		_, err = taskGroup.Wait()
		if err != nil {
			return err
		}

		return c.Properties.Alter(Property.StateV1, func(clonable data.Clonable) error {
			stateV1, ok := clonable.(*propertiesv1.State)
			if !ok {
				return scerr.InconcistentError("'*propertiesv1.State' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			stateV1.State = ClusterState.Nominal
			return nil
		})
	})
}

func (c *Cluster) asyncStartHost(task concurrency.Task, params concurrency.TaskParameters) (concurrency.TaskResult, error) {
	//FIXME: valid params
	return nil, c.Service().StartHost(params.(string))
}

// Stop stops the cluster
func (c *Cluster) Stop(task concurrency.Task) (err error) {
	if c == nil {
		return scerr.InvalidInstanceError()
	}
	if task == nil {
		return scerr.InvalidParameterError("task", "cannot be nil")
	}

	tracer := concurrency.NewTracer(nil, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	var prevState ClusterState
	err = c.Inspect(task, func(clonable data.Clonable) error {
		ctrl, ok := clonable.(cluster.Controller)
		if !ok {
			return scerr.InconsistentError("'cluster.Controller' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		var inErr error
		prevState, inErr = ctrl.ForceGetState(task)
		return inErr
	})
	if err != nil {
		return err
	}

	// If the cluster is stpped, do nothing
	if prevState == ClusterState.Stopped {
		return nil
	}

	// If the cluster is already stopping, wait for it to terminate the procedure
	if prevState == ClusterState.Stopping {
		err = retry.WhileUnsuccessfulDelay5Seconds(
			func() error {
				var state ClusterState.Enum
				inErr := c.Inspect(task, func(clonable data.Clonable) error {
					return c.Properties.Inspect(propertiesv1.State, func(clonable data.Clonable) error {
						stateV1, ok := clonable.(*propertiesv1.State)
						if !ok {
							return scerr.InconsistentError("'*propertiesv1.State' expected, '%s' provided", reflect.TypeOf(clonable).String())
						}
						state = stateV1.State
						return nil
					})
				})
				if inErr != nil {
					return inErr
				}
				if state != ClusterState.Stopped {
					return scerr.NotAvailableError("current state of cluster is '%s'", state.String())
				}
				return nil
			},
			5*time.Minute, // FIXME: static timeout
		)
		if err != nil {
			switch err.(type) {
			case retry.ErrTimeout:
				err := scerr.Wrap(err, "timeout waiting cluster transitioning from state Stopping to Stopped")
			}
			return err
		}
		return nil
	}

	// If the cluster is not in state Nominal or Degraded, can't stop
	if prevState != ClusterState.Nominal || prevState != ClusterState.Degraded {
		return scerr.NotAvailableError("failed to stop cluster because of it's current state: %s", prevState.String())
	}

	// First mark cluster to be in state Stopping
	err = c.Alter(task, func(clonable data.Clonable) error {
		return c.Properties.Alter(Property.StateV1, func(clonable data.Clonable) error {
			stateV1, ok := (*clusterpropertiesv1.State)
			if !ok {
				return scerr.InconsistentError("'*propertiesv1.State' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			stateV1.State = ClusterState.Stopping
		})
	})
	if err != nil {
		return err
	}

	// Then stop it and mark it as STOPPED on success
	return c.Alter(task, func(clonable data.Clonable) error {
		var (
			nodes                         []*propertiesv2.Node
			masters                       []*propertiesv2.Node
			gatewayID, secondaryGatewayID string
		)
		inErr := c.Properties.Inspect(Property.NodesV2, func(v interface{}) error {
			nodesV2, ok := v.(*propertiesv2.Nodes)
			if !ok {
				return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			masters = nodesV2.Masters
			nodes = nodesV2.PrivateNodes
			return nil
		})
		if inErr != nil {
			return scerr.Wrap(inErr, "failed to get list of hosts")
		}

		if c.Properties.Lookup(Property.NetworkV2) {
			inErr = c.Properties.Inspect(Property.NetworkV2, func(clonable data.Clonable) error {
				networkV2, ok := clonable.(*propertiesv2.Network)
				if !ok {
					return scerr.InconsistentError("'*propertiesv2.Network' expected, '%s' provided", reflect.TypeOf(clonable).String())
				}
				gatewayID = networkV2.GatewayID
				secondaryGatewayID = networkV2.SecondaryGatewayID
				return nil
			})
		} else {
			inErr = c.Properties.Inspect(Property.NetworkV1, func(clonable data.Clonable) error {
				networkV1, ok := clonable.(*propertiesv1.Network)
				if !ok {
					return scerr.InconsistentError("'*propertiesv1.Network' expected, '%s' provided", reflect.TypeOf(clonable).String())
				}
				gatewayID = networkV1.GatewayID
				return nil
			})
		}
		if inErr != nil {
			return inErr
		}

		// Stop nodes
		taskGroup, inErr := concurrency.NewTaskGroup(task)
		if inErr != nil {
			return inErr
		}

		// FIXME introduce status

		for _, n := range nodes {
			_, inErr = taskGroup.Start(c.asyncStopHost, n.ID)
			if inErr != nil {
				return inErr
			}
		}
		// Stop masters
		for _, n := range masters {
			_, inErr = taskGroup.Start(c.asyncStopHost, n.ID)
			if inErr != nil {
				return inErr
			}
		}
		// Stop gateway(s)
		_, inErr = taskGroup.Start(c.asyncStopHost, gatewayID)
		if inErr != nil {
			return inErr
		}
		if secondaryGatewayID != "" {
			_, inErr = taskGroup.Start(c.asyncStopHost, secondaryGatewayID)
			if inErr != nil {
				return inErr
			}
		}

		_, inErr = taskGroup.Wait()
		if inErr != nil {
			return inErr
		}

		return c.Properties.Alter(Property.StateV1, func(clonable data.Clonable) error {
			stateV1, ok := clonable.(*propertiesv1.State)
			if !ok {
				return scerr.InconcistentError("'*propertiesv1.State' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			stateV1.State = ClusterState.Stopped
			return nil
		})
	})
}

func (c *Cluster) asyncStopHost(task concurrency.Task, params concurrency.TaskParameters) (concurrency.TaskResult, error) {
	//FIXME: valid params
	return nil, c.Service().StopHost(params.(string))
}

// State returns the current state of the cluster
func (c *Cluster) State(task concurrency.Task) (state ClusterState.Enum, err error) {
	state = ClusterState.Unknown
	if c == nil {
		return state, scerr.InvalidInstanceError()
	}
	if task == nil {
		return state, scerr.InvalidParameterError("task", "cannot be nil")
	}

	tracer := concurrency.NewTracer(nil, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	var changed bool
	err = c.Alter(task, func(clonable data.Clonable) error {
		ctrl, ok := clonable.(cluster.Controller)
		if !ok {
			return scerr.InconsistentError("'cluster.Controller' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		var inErr error
		state, changed, inErr = ctrl.State(task)
		return inErr
	})
	if err != nil {
		return state, err
	}

	if changed {
		err = c.Alter(task, func(clonable data.Clonable) error {
			ctrl, ok := clonable.(cluster.Controller)
			if !ok {
				return scerr.InconsistentError("'cluster.Controller' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			props, err := ctrl.Properties(task)
			if err != nil {
				return err
			}
			return props.Alter(Property.StateV1, func(clonable data.Clonable) error {
				stateV1, ok := clonable.(*propertiesv1.State)
				if !ok {
					return scerr.InconsistentError("'*propertiesv1.State' expected, '%s' provided", reflect.TypeOf(clonable).String())
				}
				stateV1.State = state
				c.lastStateCollection = time.Now()
				return nil
			})
		})
	}
	return state, err
}

// AddNode adds a node
//
// satisfies interface cluster.Controller
func (c *Cluster) AddNode(task concurrency.Task, def *abstracts.HostDefinition) (_ *Host, err error) {
	if c == nil {
		return nil, scerr.InvalidInstanceError()
	}
	if task == nil {
		return nil, scerr.InvalidParameterError("task", "cannot be nil")
	}
	if def == nil {
		return nil, scerr.InvalidParameterError("def", "cannot be nil")
	}

	tracer := concurrency.NewTracer(nil, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	nodes, err := c.AddNodes(task, 1, def)
	if err != nil {
		return nil, err
	}

	svc := c.Service()
	return LoadHost(task, svc, nodes[0].ID)
}

// AddNodes adds several nodes
func (c *Cluster) AddNodes(task concurrency.Task, count int, def *abstracts.HostDefinition) (_ []*propertiesv1.Node, err error) {
	if c == nil {
		return nil, scerr.InvalidInstanceError()
	}
	if task == nil {
		return nil, scerr.InvalidParameterError("task", "cannot be nil")
	}
	if count < 1 {
		return nil, scerr.InvalidParameterError("count", "cannot be an integer less than 1")
	}
	if def == nil {
		return nil, scerr.InvalidParameterError("def", "cannot be nil")
	}

	tracer := concurrency.NewTracer(task, fmt.Sprintf("(%d)", count), true)
	defer tracer.GoingIn().OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	nodeDef := complementHostDefinition(req, abstracts.HostDefinition{})
	var hostImage string

	properties := c.Properties
	if !properties.Lookup(Property.DefaultsV2) {
		err = c.Alter(task, func(clonable data.Clonable) error {
			return properties.Inspect(Property.DefaultsV1, func(clonable data.Clonable) error {
				defaultsV1, ok := clonable.(*propertiesv1.Defaults)
				if !ok {
					return scerr.InconsistentError("'*propertiesv1.Defaults' expected, '%s' provided", reflect.TypeOf(clonable).String())
				}
				return properties.Alter(Property.DefaultsV2, func(clonable data.Clonable) error {
					defaultsV2, ok := clonable.(*propertiesv2.Defaults)
					if !ok {
						return scerr.InconsistentError("'*propertiesv2.Defaults' expected, '%s' provided", reflect.TypeOf(clonable).String())
					}
					convertDefaultsV1ToDefaultsV2(defaultsV1, defaultsV2)
					return nil
				})
			})
		})
		if err != nil {
			return nil, err
		}
	}
	err = properties.Inspect(Property.DefaultsV2, func(clonable data.Clonable) error {
		defaultsV2, ok := v.(*propertiesv2.Defaults)
		if !ok {
			return scerr.InconsistentError("'*propertiesv2.Defaults' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		sizing := srvutils.ToPBHostSizing(defaultsV2.NodeSizing)
		nodeDef.Sizing = &sizing
		hostImage = defaultsV2.Image
		return nil
	})
	if err != nil {
		return nil, err
	}

	if nodeDef.ImageId == "" {
		nodeDef.ImageId = hostImage
	}

	var (
		nodeType    NodeType.Enum
		nodeTypeStr string
		errors      []string
	)
	netCfg, err := c.NetworkConfig(task)
	if err != nil {
		return nil, err
	}
	nodeDef.Network = netCfg.NetworkID

	var nodes []*propertiesv1.Node
	err = c.Alter(task, func(clonable data.Clonable) error {
		ctrl, ok := clonable.(*cluster.Controller)
		if !ok {
			return scerr.InconsistentError("'c*luster.Controller' was expected, '%s' is provided", reflect.TypeOf(clonable).String())
		}
		var innerErr error
		nodes, inErr = ctrl.AddNodes(task, count, def)
		if inErr != nil {
			return inErr
		}

		defer func() {
			if inErr != nil {
				deleteNodes(task, oc.Service(), nodes)
			}
		}()

		inErr = properties.Alter(Property.NodesV1, func(clonable data.Clonable) error {
			nodesV1, ok := clonable.(*propertiesv1.Nodes)
			if !ok {
				return scerr.InconsistentError("'*propertiesv1.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			// for _, node := range nodes {
			nodesV1.PrivateNodes = append(nodesV1.PrivateNodes, nodes...)
			// }
			return nil
		})
		if inErr != nil {
			return inErr
		}
		return ctrl.EnlistNodes(task, nodes)
	})
	if err != nil {
		return nil, err
	}
	return nodes, err
}

// complementHostDefinition complements req with default values if needed
func complementHostDefinition(req *abstracts.HostDefinition, def abstracts.HostDefinition) *abstracts.HostDefinition {
	var finalDef abstracts.HostDefinition
	if req == nil {
		finalDef = def
	} else {
		finalDef = *req
		finalDef.Sizing = &abstracts.HostSizing{}
		*finalDef.Sizing = *req.Sizing

		if def.Sizing.MinCpuCount > 0 && finalDef.Sizing.MinCpuCount == 0 {
			finalDef.Sizing.MinCpuCount = def.Sizing.MinCpuCount
		}
		if def.Sizing.MaxCpuCount > 0 && finalDef.Sizing.MaxCpuCount == 0 {
			finalDef.Sizing.MaxCpuCount = def.Sizing.MaxCpuCount
		}
		if def.Sizing.MinRamSize > 0.0 && finalDef.Sizing.MinRamSize == 0.0 {
			finalDef.Sizing.MinRamSize = def.Sizing.MinRamSize
		}
		if def.Sizing.MaxRamSize > 0.0 && finalDef.Sizing.MaxRamSize == 0.0 {
			finalDef.Sizing.MaxRamSize = def.Sizing.MaxRamSize
		}
		if def.Sizing.MinDiskSize > 0 && finalDef.Sizing.MinDiskSize == 0 {
			finalDef.Sizing.MinDiskSize = def.Sizing.MinDiskSize
		}
		if finalDef.Sizing.GpuCount <= 0 && def.Sizing.GpuCount > 0 {
			finalDef.Sizing.GpuCount = def.Sizing.GpuCount
		}
		if finalDef.Sizing.MinCpuFreq == 0 && def.Sizing.MinCpuFreq > 0 {
			finalDef.Sizing.MinCpuFreq = def.Sizing.MinCpuFreq
		}
		if finalDef.ImageId == "" {
			finalDef.ImageId = def.ImageId
		}

		if finalDef.Sizing.MinCpuCount <= 0 {
			finalDef.Sizing.MinCpuCount = 2
		}
		if finalDef.Sizing.MaxCpuCount <= 0 {
			finalDef.Sizing.MaxCpuCount = 4
		}
		if finalDef.Sizing.MinRamSize <= 0.0 {
			finalDef.Sizing.MinRamSize = 7.0
		}
		if finalDef.Sizing.MaxRamSize <= 0.0 {
			finalDef.Sizing.MaxRamSize = 16.0
		}
		if finalDef.Sizing.MinDiskSize <= 0 {
			finalDef.Sizing.MinDiskSize = 50
		}
	}

	return &finalDef
}

func convertDefaultsV1ToDefaultsV2(defaultsV1 *propertiesv1.Defaults, defaultsV2 *propertiesv2.Defaults) {
	defaultsV2.Image = defaultsV1.Image
	defaultsV2.MasterSizing = abstracts.SizingRequirements{
		MinCores:    defaultsV1.MasterSizing.Cores,
		MinFreq:     defaultsV1.MasterSizing.CPUFreq,
		MinGPU:      defaultsV1.MasterSizing.GPUNumber,
		MinRAMSize:  defaultsV1.MasterSizing.RAMSize,
		MinDiskSize: defaultsV1.MasterSizing.DiskSize,
		Replaceable: defaultsV1.MasterSizing.Replaceable,
	}
	defaultsV2.NodeSizing = abstracts.SizingRequirements{
		MinCores:    defaultsV1.NodeSizing.Cores,
		MinFreq:     defaultsV1.NodeSizing.CPUFreq,
		MinGPU:      defaultsV1.NodeSizing.GPUNumber,
		MinRAMSize:  defaultsV1.NodeSizing.RAMSize,
		MinDiskSize: defaultsV1.NodeSizing.DiskSize,
		Replaceable: defaultsV1.NodeSizing.Replaceable,
	}
}

// DeleteLastNode deletes the last added node and returns its name
func (c *Cluster) DeleteLastNode(task concurrency.Task) (node *propertiesv1.Node, err error) {
	if c == nil {
		return nil, scerr.InvalidInstanceError()
	}
	if task == nil {
		return nil, scerr.InvalidParameterError("task", "cannot be nil")
	}

	tracer := concurrency.NewTracer(nil, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	selectedMaster, err := c.FindAvailableMaster(task)
	if err != nil {
		return nil, err
	}

	// Removed reference of the node from cluster
	err = c.Alter(task, func(clonable data.Clonable) error {
		ctrl, ok := clonable.(*cluster.Controller)
		if !ok {
			return scerr.InconsistentError("'*cluster.Controller' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}

		inErr = c.Properties.Inspect(Property.NodesV1, func(clonable data.Clonable) error {
			nodesV2, ok := clonable.(*propertiesv2.Nodes)
			if !ok {
				return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			node = nodesV2.PrivateNodes[len(nodesV2.PrivateNodes)-1]
			return nil
		})
		if inErr != nil {
			return inErr
		}
		return c.DeleteSpecificNode(task, node.ID, selectedMaster.ID)
	})
	if err != nil {
		return nil, err
	}
	return node, nil
}

// DeleteSpecificNode deletes a node identified by its ID
func (c *Cluster) DeleteSpecificNode(task concurrency.Task, hostID string, selectedMasterID string) (err error) {
	if c == nil {
		return scerr.InvalidInstanceError()
	}
	if task == nil {
		return scerr.InvalidParameterError("task", "cannot be nil")
	}
	if hostID == "" {
		return scerr.InvalidParameterError("hostID", "cannot be empty string")
	}

	tracer := concurrency.NewTracer(nil, "", false).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	var node *propertiesv2.Node

	return c.Alter(task, func(clonable data.Clonable) error {
		ctrl, ok := clonable.(cluster.Controller)
		if !ok {
			return scerr.InconsistentError("'cluster.Controller' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		inErr := c.Properties.Inspect(Property.NodesV2, func(clonable data.Clonable) error {
			nodesV2, ok := clonable.(*propertiesv2.Nodes)
			if !ok {
				return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			found, idx := contains(nodesV2.PrivateNodes, hostID)
			if !found {
				return scerr.NotFoundError("failed to find host '%s'", hostID)
			}
			node = nodesV2.PrivateNodes[idx]
			return nil
		})
		if inErr != nil {
			return inErr
		}

		if selectedMasterID == "" {
			selectedMaster, inErr = c.FindAvailableMaster(task)
			if err != nil {
				return inErr
			}
			selectedMasterID = selectedMaster.ID
		}

		inErr = ctrl.DeleteNode(task, node.ID, selectedMasterID)
		if inErr != nil {
			return inErr
		}

		// Removes node from cluster metadata (done before really deleting node to prevent operations on the node in parallel)
		return c.Properties.Alter(Property.NodesV2, func(clonable data.Clonable) error {
			nodesV2, ok := v.(*propertiesv2.Nodes)
			if !ok {
				return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			length := len(nodesV2.PrivateNodes)
			_, idx := contains(nodesV2.PrivateNodes, node.ID)
			if idx < length-1 {
				nodesV2.PrivateNodes = append(nodesV2.PrivateNodes[:idx], nodesV2.PrivateNodes[idx+1:]...)
			} else {
				nodesV2.PrivateNodes = nodesV2.PrivateNodes[:idx]
			}
			return nil
		})
	})
}

// ListMasters lists the node instances corresponding to masters (if there is such masters in the flavor...)
func (oc *Cluster) ListMasters(task concurrency.Task) (list resources.IndexedListOfClusterNodes, err error) {
	if oc == nil {
		return nil, scerr.InvalidInstanceError()
	}
	defer scerr.OnPanic(&err)()

	if task == nil {
		task, err = concurrency.RootTask()
		if err != nil {
			return nil, err
		}
	}

	list = cluster.NodeList{}
	err = c.Properties.Inspect(Property.NodesV2, func(clonable data.Clonable) error {
		nodesV2, ok := clonable.(*propertiesv2.Nodes)
		if !ok {
			return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		for _, i := range nodesV2.Masters {
			list[i.NumericalID] = i
		}
		return nil
	})
	if err != nil {
		logrus.Errorf("failed to get list of master names: %v", err)
		return list, err
	}
	return list, err
}

// ListMasterNames lists the names of the master nodes in the Cluster
func (c *Cluster) ListMasterNames(task concurrency.Task) (list data.IndexedListOfStrings, err error) {
	if oc == nil {
		return nil, scerr.InvalidInstanceError()
	}
	defer scerr.OnPanic(&err)()

	if task == nil {
		task, err = concurrency.RootTask()
		if err != nil {
			return nil, err
		}
	}

	list = cluster.NodeList{}
	err = c.Properties.Inspect(Property.NodesV2, func(clonable data.Clonable) error {
		nodesV2, ok := v.(*propertiesv2.Nodes).Masters
		if !ok {
			return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		for _, i := range nodesV2 {
			list[i.NumericalID] = v.Name
		}
		return nil
	})
	if err != nil {
		// logrus.Errorf("failed to get list of master names: %v", err)
		return nil, err
	}
	return list, nil
}

// ListMasterIDs lists the IDs of masters (if there is such masters in the flavor...)
func (c *Cluster) ListMasterIDs(task concurrency.Task) (list data.IndexedListOfStrings, err error) {
	if c == nil {
		return nil, scerr.InvalidInstanceError()
	}
	defer scerr.OnPanic(&err)()

	if task == nil {
		task, err = concurrency.RootTask()
		if err != nil {
			return nil, err
		}
	}

	list = cluster.NodeList{}
	err = c.Properties.Inspect(Property.NodesV2, func(clonable data.Clonable) error {
		nodesV2, ok := clonable.(*propertiesv2.Nodes).Masters
		if !ok {
			return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		for _, i := range nodesV2 {
			list[i.NumericalID] = v.ID
		}
		return nil
	})
	if err != nil {
		return nil, scerr.Wrap(err, "failed to get list of master IDs")
	}
	return list, nil
}

// ListMasterIPs lists the IPs of masters (if there is such masters in the flavor...)
func (oc *Cluster) ListMasterIPs(task concurrency.Task) (list []string, err error) {
	if oc == nil {
		return nil, scerr.InvalidInstanceError()
	}
	defer scerr.OnPanic(&err)()

	if task == nil {
		task, err = concurrency.RootTask()
		if err != nil {
			return nil, err
		}
	}

	list = cluster.NodeList{}
	err = c.Properties.Inspect(Property.NodesV2, func(v data.Clonable) error {
		nodesV2, ok := clonable.(*propertiesv2.Nodes).Masters
		if !ok {
			return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		for _, i := range nodesV2 {
			list[i.NumericalID] = v.PrivateIP
		}
		return nil
	})
	if err != nil {
		logrus.Errorf("failed to get list of master IPs: %v", err)
		return nil, err
	}
	return list, err
}

// FindAvailableMaster returns ID of the first master available to execute order
// satisfies interface cluster.cluster.Controller
func (c *Cluster) FindAvailableMaster(task concurrency.Task) (master *propertiesv2.Node, err error) {
	if c == nil {
		return nil, scerr.InvalidInstanceError()
	}
	defer scerr.OnPanic(&err)()
	defer scerr.OnExitLogError("failed to find available master", &err)()

	if task == nil {
		task, err = concurrency.RootTask()
		if err != nil {
			return nil, err
		}
	}

	tracer := concurrency.NewTracer(task, "", true).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	found := false
	masters, err := c.ListMasters(task)
	if err != nil {
		return nil, err
	}

	var lastError error
	svc := c.Service()
	for _, m = range masters {
		host, err := LoadHost(svc, m.ID)
		if err != nil {
			return nil, err
		}

		ctx, err := task.GetContext()
		if err != nil {
			logrus.Errorf("failed to get context: %s", err.Error())
			continue
		}

		_, err = host.WaitServerReady(ctx, "ready", temporal.GetConnectSSHTimeout())
		if err != nil {
			if _, ok := err.(retry.ErrTimeout); ok {
				lastError = err
				continue
			}
			return nil, err
		}
		found = true
		master = m
		break
	}
	if !found {
		return nil, fmt.Errorf("failed to find available master: %v", lastError)
	}
	return master, nil
}

// ListNodes lists node instances corresponding to the nodes in the cluster
// satisfies interface cluster.Controller
func (c *Cluster) ListNodes(task concurrency.Task) (list resources.IndexedListOfClusterNodes, err error) {
	if c == nil {
		return nil, scerr.InvalidInstanceError()
	}
	defer scerr.OnPanic(&err)()

	if task == nil {
		task, err = concurrency.RootTask()
		if err != nil {
			return nil, err
		}
	}

	list = IndexedListOfClusterNodes{}
	err = c.Properties.Inspect(clusterproperty.NodesV2, func(clonable data.Clonable) error {
		nodesV2, ok := clonable.(*propertiesv2.Nodes)
		if !ok {
			return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		for _, v := range nodesV2.PrivateNodes {
			list[v.NumericalID] = v.Name
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return list, nil
}

// ListNodeNames lists the names of the nodes in the Cluster
func (oc *Cluster) ListNodeNames(task concurrency.Task) (list data.IndexedListOfStrings, err error) {
	if oc == nil {
		return nil, scerr.InvalidInstanceError()
	}
	defer scerr.OnPanic(&err)()

	if task == nil {
		task, err = concurrency.RootTask()
		if err != nil {
			return nil, err
		}
	}

	list = cluster.NodeList{}
	err = oc.Properties.Inspect(clusterproperty.NodesV2, func(clonable data.Clonable) error {
		nodesV2, ok := clonable.(*propertiesv2.Nodes).PrivateNodes
		if !ok {
			return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		for _, i := range nodesV2 {
			list[i.NumericalID] = v.Name
		}
		return nil
	})
	if err != nil {
		// logrus.Errorf("failed to get list of node IDs: %v", err)
		return nil, err
	}
	return list, err
}

// ListNodeIDs lists IDs of the nodes in the cluster
func (c *Cluster) ListNodeIDs(task concurrency.Task) (list cluster.NodeStringList, err error) {
	if c == nil {
		return nil, scerr.InvalidInstanceError()
	}
	defer scerr.OnExitLogError("failed to get list of node IDs", &err)()

	if task == nil {
		return nil, scerr.InvalidParameterError("task", "cannot be nil")
	}

	list = cluster.NodeStringList{}
	err = c.Properties.Inspect(Property.NodesV2, func(clonable data.Clonable) error {
		nodesV2, ok := clonable.(*propertiesv2.Nodes)
		if !ok {
			return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		for _, i := range nodesV2.PrivateNodes {
			list[v.NumericalID] = v.ID
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return list, err
}

// ListNodeIPs lists the IPs of the nodes in the cluster
// satisfies interface cluster.cluster.Controller
func (c *Cluster) ListNodeIPs(task concurrency.Task) (list cluster.NodeStringList, err error) {
	if c == nil {
		return nil, scerr.InvalidInstanceError()
	}
	defer scerr.OnExitLogError("failed to get list of node IP addresses", &err)()
	defer scerr.OnPanic(&err)()

	if task == nil {
		return nil, scerr.InvalidParameterError("task", "cannot be nil")
	}

	list = cluster.NodeStringList{}
	err = c.Properties.Inspect(Property.NodesV2, func(clonable data.Clonable) error {
		nodesV2, ok := clonable.(*propertiesv2.Nodes).PrivateNodes
		if !ok {
			return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		for _, i := range nodesV2 {
			list[v.NumericalID] = i.PrivateIP
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return list, nil
}

// FindAvailableNode returns node instance of the first node available to execute order
// satisfies interface cluster.cluster.Controller
func (c *Cluster) FindAvailableNode(task concurrency.Task) (node *propertiesv2.Node, err error) {
	if c == nil {
		return "", scerr.InvalidInstanceError()
	}
	defer scerr.OnPanic(&err)()

	if task == nil {
		return "", scerr.InvalidParameterError("task", "cannot be nil")
	}

	tracer := concurrency.NewTracer(task, "", true).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	list, err := c.ListNodes(task)
	if err != nil {
		return nil, err
	}

	found := false
	svc := c.Service()
	for _, n := range list {
		host, err := LoadHost(svc, n.ID)
		if err != nil {
			return nil, err
		}

		err = host.WaitServerReady(task, "ready", temporal.GetConnectSSHTimeout())
		if err != nil {
			if _, ok := err.(retry.ErrTimeout); ok {
				continue
			}
			return nil, err
		}
		found = true
		node = n
		break
	}
	if !found {
		return nil, fmt.Errorf("failed to find available node")
	}
	return node, nil
}

// LookupNode tells if the ID of the host passed as parameter is a node
// satisfies interface cluster.cluster.Controller
func (c *Cluster) LookupNode(task concurrency.Task, ref string) (found bool, err error) {
	if c == nil {
		return false, scerr.InvalidInstanceError()
	}
	if ref == "" {
		return false, scerr.InvalidParameterError("ref", "cannot be empty string")
	}
	defer scerr.OnPanic(&err)()

	if task == nil {
		task = concurrency.RootTask()
	}

	found = false
	err = c.Properties.Inspect(Property.NodesV2, func(clonable data.Clonable) error {
		nodesV2, ok := clonable.(*propertiesv2.Nodes)
		if !ok {
			return scerr.InconcistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		found, _ = contains(nodesV2.PrivateNodes, hostID)
		return nil
	})

	return found, err
}

// CountNodes counts the nodes of the cluster
// satisfies interface cluster.cluster.Controller
func (c *Cluster) CountNodes(task concurrency.Task) (count uint, err error) {
	if c == nil {
		return 0, scerr.InvalidInstanceError()
	}
	defer scerr.OnExitLogError(concurrency.NewTracer(task, "", concurrency.IsLogActive("Trace.Controller")).TraceMessage(""), &err)()

	if task == nil {
		return 0, scerr.InvalidParameterError("task", "cannot be nil")
	}

	err = c.Properties.Inspect(Property.NodesV2, func(clonable data.Clonable) error {
		nodesV2, ok := clonable.(*propertiesv2.Nodes)
		if !ok {
			return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		count = uint(len(nodesV2.PrivateNodes))
		return nil
	})
	if err != nil {
		err = scerr.Wrap(err, "failed to count nodes")
		return 0, err
	}
	return count, nil
}

// Node returns a node based on its ID
func (c *Cluster) Node(task concurrency.Task, hostID string) (host *resources.Host, err error) {
	if c == nil {
		return nil, scerr.InvalidInstanceError()
	}
	if hostID == "" {
		return nil, scerr.InvalidParameterError("hostID", "cannot be empty string")
	}
	defer scerr.OnPanic(&err)()

	if task == nil {
		task = concurrency.RootTask()
	}

	tracer := concurrency.NewTracer(task, fmt.Sprintf("(%s)", hostID), true)
	defer tracer.GoingIn().OnExitTrace()()
	defer scerr.OnExitLogError(fmt.Sprintf("failed to get node identified by '%s'", hostID), &err)()

	found := false
	err = c.Properties.Inspect(Property.NodesV2, func(clonable data.Clonable) error {
		nodesV2, ok := clonable.(*propertiesv2.Nodes)
		if !ok {
			return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
		}
		found, _ = contains(nodesV2.PrivateNodes, hostID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("failed to find node '%s' in Cluster '%s'", hostID, c.Name)
	}
	return resources.LoadHost(c.Service(), hostID)
}

// deleteMaster deletes the master specified by its ID
func (c *Cluster) deleteMaster(task concurrency.Task, hostID string) error {
	if c == nil {
		return scerr.InvalidInstanceError()
	}
	if hostID == "" {
		return scerr.InvalidParameterError("hostID", "cannot be empty string")
	}

	var master *propertiesv1.Node
	// Removes master from cluster properties
	err := c.Alter(task, func(clonable data.Clonable) error {
		return c.Properties.Alter(Property.NodesV1, func(clonable data.Clonable) error {
			nodesV1, ok := clonable.(*propertiesv1.Nodes)
			if !ok {
				return scerr.InconsistentError("'*propertiesv1.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			found, idx := contains(nodesV1.Masters, hostID)
			if !found {
				return scerr.ResourceNotFoundError("host", hostID)
			}
			master = nodesV1.Masters[idx]
			if idx < len(nodesV1.Masters)-1 {
				nodesV1.Masters = append(nodesV1.Masters[:idx], nodesV1.Masters[idx+1:]...)
			} else {
				nodesV1.Masters = nodesV1.Masters[:idx]
			}
			return nil
		})
	})
	if err != nil {
		return err
	}

	// // Starting from here, restore master in cluster properties if exiting with error
	// defer func() {
	// 	if err != nil {
	// 		derr := c.Alter(task, func(clonable data.Clonable) error {
	// 				innerErr = c.Properties.Alter(Property.NodesV2, func(clonable data.Clonable) error {
	// 					nodesV2 := clonable.(*propertiesv2.Nodes)
	// 					nodesV2.Masters = append(nodesV2.Masters, master)
	// 					return nil
	// 				})
	// 			}
	// 			return innerErr
	// 		})
	// 		if derr != nil {
	// 			logrus.Errorf("After failure, cleanup failed to restore master '%s' in cluster", master.Name)
	// 		}
	// 	}
	// }()

	// Finally delete host
	return c.Service().DeleteHost(master.ID)
}

// Delete allows to destroy infrastructure of cluster
// satisfies interface cluster.Controller
func (c *Cluster) Delete(task concurrency.Task) error {
	if c == nil {
		return scerr.InvalidInstanceError()
	}
	if task == nil {
		return scerr.InvalidParameterError("task", "cannot be nil")
	}

	err := c.Alter(task, func(clonable data.Clonable) error {
		// Updates cluster state to mark cluster as Removing
		return c.Properties.Alter(Property.StateV1, func(clonable data.Clonable) error {
			stateV1, ok := clonable.(*propertiesv1.State)
			if !ok {
				return scerr.InconsistentError("'*propertiesv1.State' expected, '%s' provided", reflect.TypeOf(clonable).String())
			}
			stateV1.State = ClusterState.Removed
			return nil
		})
	})
	if err != nil {
		return err
	}

	taskDeleteNode := func(t concurrency.Task, params concurrency.TaskParameters) (concurrency.TaskResult, error) {
		err := c.DeleteSpecificNode(t, params.(string), "")
		return nil, err
	}

	taskDeleteMaster := func(t concurrency.Task, params concurrency.TaskParameters) (concurrency.TaskResult, error) {
		err := c.deleteMaster(t, params.(string))
		return nil, err
	}

	var cleaningErrors []error

	// deletes the cluster
	err = c.Alter(task, func(clonable data.Clonable) error {
		ctrl := clonable.(cluster.Controller)
		props, innerErr := ctrl.Properties(task)
		if innerErr != nil {
			return innerErr
		}

		// Deletes the nodes
		list, innerErr := c.ListNodes(task)
		if err != nil {
			return innerErr
		}
		length := len(list)
		if length > 0 {
			subtasks := make([]concurrency.Task, 0, length)
			for i := 0; i < length; i++ {
				subtask, innerErr = task.StartInSubtask(taskDeleteNode, list[i])
				if innerErr != nil {
					cleaningErrors = append(cleaningErrors, scerr.Wrap(innerErr, fmt.Sprintf("failed to start deletion of node '%s'", list[i].Name)))
					break
				}
				subtasks = append(subtasks, subtask)
			}
			for _, s := range subtasks {
				_, subErr = s.Wait()
				if subErr != nil {
					cleaningErrors = append(cleaningErrors, subErr)
				}
			}
		}

		// Delete the Masters
		list, innerErr = c.ListMasters(task)
		if innerErr != nil {
			cleaningErrors = append(cleaningErrors, innerErr)
			return scerr.ErrListError(cleaningErrors)
		}
		length = len(list)
		if length > 0 {
			subtasks := make([]concurrency.Task, 0, length)
			for i := 0; i < length; i++ {
				subtask, innerErr = task.StartInSubTask(taskDeleteMaster, list[i])
				if innerErr != nil {
					cleaningErrors = append(cleaningErrors, scerr.Wrap(innerErr, fmt.Sprintf("failed to start deletion of master '%s'", list[i].Name)))
					break
				}
				subtasks = append(subtasks, subtask)
			}
			for _, s := range subtasks {
				_, subErr := s.Wait()
				if subErr != nil {
					cleaningErrors = append(cleaningErrors, subErr)
				}
			}
		}

		// Deletes the network and gateway
		// c.RLock(task)
		networkID := ""
		if c.Properties.Lookup(Property.NetworkV2) {
			err = c.Properties.Inspect(Property.NetworkV2, func(v interface{}) error {
				networkID = v.(*propertiesv2.Network).NetworkID
				return nil
			})
		} else {
			err = c.Properties.Inspect(Property.NetworkV1, func(v interface{}) error {
				networkID = v.(*propertiesv1.Network).NetworkID
				return nil
			})
		}
		// c.RUnlock(task)
		if innerErr != nil {
			cleaningErrors = append(cleaningErrors, err)
			return innerErr
		}

		network, innerErr := LoadNetwork(task, c.Service(), networkID)
		if innerErr != nil {
			cleaningErrors = append(cleaningErrors, err)
			return innerErr
		}
		return retry.WhileUnsuccessfulDelay5SecondsTimeout(
			func() error {
				return network.Delete(task)
			},
			temporal.GetHostTimeout(),
		)
	})
	if err != nil {
		return scerr.ErrListError(cleaningErrors)
	}

	return c.core.Delete(task)
}

func deleteNodes(task concurrency.Task, svc iaas.Service, nodes []*propertiesv2.Node) {
	length := len(nodes)
	if length > 0 {
		subtasks := make([]concurrency.Task, 0, length)
		for i := 0; i < length; i++ {
			host, err := LoadHost(task, svc, nodes[i].ID)
			if err != nil {
				subtasks[i] = nil
				logrus.Errorf(err.Error())
				continue
			}
			subtask, err := task.New().Start(
				func(task concurrency.Task, params concurrency.TaskParameters) (concurrency.TaskResult, error) {
					return nil, host.Delete(task)
				},
				nil,
			)
			if err != nil {
				subtasks[i] = nil
				logrus.Errorf(err.Error())
				continue
			}
			subtasks[i] = subtask
		}
		for i := 0; i < length; i++ {
			if subtasks[i] != nil {
				state := subtasks[i].Wait()
				if state != nil {
					logrus.Errorf("after failure, cleanup failed to delete node '%s': %v", nodes[i].Name, state)
				}
			}
		}
	}
}

// TargetType returns the type of the target
//
// satisfies install.Targetable interface
func (oc *Cluster) TargetType() string {
	return "cluster"
}

// InstallMethods returns a list of installation methods useable on the target, ordered from upper to lower preference (1 = highest preference)
// satisfies feature.Targetable interface
func (c *Cluster) InstallMethods(task concurrency.Task) map[uint8]installmethod.Enum {
	if c == nil {
		logrus.Error(scerr.InvalidInstanceError().Error())
		return nil
	}
	if task == nil {
		logrus.Errorf(scerr.InvalidParameterError("task", "cannot be nil").Error())
		return nil
	}

	task.Lock(c.core.lock)
	defer task.Unlock(c.core.lock)

	if c.installMethods == nil {
		c.installMethods = map[uint8]installmethod.Enum{}
		var index uint8
		_ = c.Inspect(task, func(clonable data.Clonable) error {
			controller := clonable.(cluster.Controller)
			flavor, err := controller.Flavor()
			if err == nil {
				switch flavor {
				case Flavor.DCOS:
					index++
					mc.installMethods[index] = installmethod.DCOS
				case Flavor.K8S:
					index++
					mc.installMethods[index] = installmethod.Helm
				}
			}
		})
		index++
		c.installMethods[index] = installmethod.Bash
	}
	return c.installMethods
}

// InstalledFeatures returns a list of installed features
func (c *Cluster) InstalledFeatures(task concurrency.Task) []string {
	var list []string
	return list
}

// ComplementFeatureParameters configures parameters that are implicitely defined, based on target
func (c *Cluster) ComplementFeatureParameters(task concurrency.Task, v data.Map) error {
	return mc.Inspect(task, func(clonable data.Clonable) error {
		var err error
		controller := clonable.(cluster.Controller)
		if v["ClusterName"], err = controller.Name(); err != nil {
			return err
		}
		var complexity Complexity.Enum
		if complexity, err = controller.Complexity(); err != nil {
			return err
		}
		v["ClusterComplexity"] = strings.ToLower(complexity.String())
		var flavor ClusterFlavor.Enum
		if flavor, err = controller.Flavor(); err != nil {
			return err
		}
		v["ClusterFlavor"] = strings.ToLower(flavor.String())
		networkCfg, err := controller.NetworkConfig(task)
		if err != nil {
			return err
		}
		v["GatewayIP"] = networkCfg.GatewayIP
		v["PublicIP"] = networkCfg.PublicIP
		v["MasterIDs"], err = controller.ListMasterIDs(task)
		if err != nil {
			return err
		}
		v["MasterIPs"], err = controller.ListMasterIPs(task)
		if err != nil {
			return err
		}
		if _, ok := v["Username"]; !ok {
			v["Username"] = "cladm"
		}
		if _, ok := v["Password"]; !ok {
			if v["Password"], err = controller.AdminPassword(); err != nil {
				return err
			}
		}
		if _, ok := v["CIDR"]; !ok {
			v["CIDR"] = networkCfg.CIDR
		}
		return nil
	})
}

func contains(list []*propertiesv2.Node, hostID string) (bool, int) {
	var idx int
	found := false
	for i, v := range list {
		if v.ID == hostID {
			found = true
			idx = i
			break
		}
	}
	return found, idx
}

// AddFeature installs a feature on the cluster
func (c *Cluster) AddFeature(task concurrency.Task, name string, vars data.Map, settings resources.InstallSettings) (resources.InstallResults, error) {
	if c == nil {
		return nil, scerr.InvalidInstanceError().Error()
	}
	if task == nil {
		return nil, scerr.InvalidParameterError("task", "cannot be nil")
	}
	if name == "" {
		return nil, scerr.InvalidParameterError("name", "cannot be empty string")
	}

	feature, err := features.New(task, name)
	if err != nil {
		return nil, err
	}
	return feature.Add(c, vars, settings)
}

// CheckFeature tells if a feature is installed on the cluster
func (c *Cluster) CheckFeature(task concurrency.Task, name string, vars data.Map, settings resources.InstallSettings) (resources.InstallResults, error) {
	if c == nil {
		return nil, scerr.InvalidInstanceError().Error()
	}
	if task == nil {
		return nil, scerr.InvalidParameterError("task", "cannot be nil")
	}
	if name == "" {
		return nil, scerr.InvalidParameterError("name", "cannot be empty string")
	}

	feature, err := features.New(task, name)
	if err != nil {
		return nil, err
	}

	return feature.Check(c, vars, settings)
}

// DeleteFeature uninstalls a feature from the cluster
func (c *Cluster) DeleteFeature(task concurrency.Task, name string, vars data.Map, settings resources.InstallSettings) (resources.InstallResults, error) {
	if c == nil {
		return nil, scerr.InvalidInstanceError().Error()
	}
	if task == nil {
		return nil, scerr.InvalidParameterError("task", "cannot be nil")
	}
	if name == "" {
		return nil, scerr.InvalidParameterError("name", "cannot be empty string")
	}

	feature, err := features.New(task, name)
	if err != nil {
		return nil, err
	}

	return feature.Delete(c, vars, settings)
}
