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
	"context"
	"fmt"
	"strings"

	rice "github.com/GeertJohan/go.rice"
	"github.com/sirupsen/logrus"

	"github.com/CS-SI/SafeScale/lib/client"
	"github.com/CS-SI/SafeScale/lib/server/iaas"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clustercomplexity"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clusterflavor"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clusterstate"
	"github.com/CS-SI/SafeScale/lib/server/resources/payloads/clusters/flavors/boh"
	"github.com/CS-SI/SafeScale/lib/server/resources/payloads/clusters/flavors/k8s"
	propertiesv2 "github.com/CS-SI/SafeScale/lib/server/resources/properties/v2"
	"github.com/CS-SI/SafeScale/lib/system"
	"github.com/CS-SI/SafeScale/lib/utils"
	"github.com/CS-SI/SafeScale/lib/utils/concurrency"
	"github.com/CS-SI/SafeScale/lib/utils/data"
	"github.com/CS-SI/SafeScale/lib/utils/scerr"
	"github.com/CS-SI/SafeScale/lib/utils/serialize"
	"github.com/CS-SI/SafeScale/lib/utils/temporal"
)

// controller contains the information about a cluster
type controller struct {
	resources.ClusterIdentity

	// Properties *serialize.JSONProperties `json:"properties,omitempty"` // Properties contains additional info about the cluster

	// foreman *foreman
	// metadata *Metadata
	service iaas.Service
	makers  Makers

	// lastStateCollection time.Time

	concurrency.TaskedLock
}

// NewController ...
func NewController(task concurrency.task, svc iaas.Service, req resources.ClusterRequest) (ctrl *controller, err error) {
	if svc == nil {
		return nil, scerr.InvalidParameterError("svc", "cannot be nil")
	}
	defer scerr.OnPanic(&err)()

	ctrl = &controller{
		Name:       req.Name,
		Flavor:     req.Flavor,
		Complexity: req.Complexity,

		service: svc,
		// metadata:   metadata,
		// Properties: serialize.NewJSONProperties("clusters"),
		TaskedLock: concurrency.NewTaskedLock(),
	}
	err = ctrl.Bootstrap(task)
	if err != nil {
		return nil, err
	}
	return ctrl, nil
}

// Boostrap (re)connects controller with the appropriate Makers
func (c *controller) Bootstrap(task concurrency.Task) error {
	c.Lock(task)
	defer c.Unlock(task)

	flavor := controller.Flavor(task)
	switch flavor {
	// case clusterflavor.DCOS:
	// 	c.makers = dcos.Makers
	case clusterflavor.BOH:
		c.makers = boh.Makers
	// case clusterflavor.OHPC:
	// 	c.makers = ohpc.Makers
	case clusterflavor.K8S:
		c.makers = k8s.Makers
	// case clusterflavor.SWARM:
	// 	c.makers = swarm.Makers
	default:
		return scerr.NotImplementedError("unknown cluster Flavor '%d'", int(flavor))
	}
	return nil
}

// func (c *Controller) replace(task concurrency.Task, src *Controller) (err error) {
// 	if c == nil {
// 		return scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	err = c.Lock(task)
// 	if err != nil {
// 		return err
// 	}
// 	defer func() {
// 		unlockErr := c.Unlock(task)
// 		if unlockErr != nil {
// 			logrus.Warn(unlockErr)
// 		}
// 		if err == nil && unlockErr != nil {
// 			err = unlockErr
// 		}
// 	}()

// 	//	(&c.Identity).Replace(&src.Identity)
// 	// c.Properties = src.Properties
// 	*c = *src
// 	return nil
// }

// // Restore restores full ability of a Cluster controller by binding with appropriate Makers
// func (c *controller) Bootstrap(task concurrency.Task) (err error) {
// 	if c == nil {
// 		return scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		return scerr.InvalidParameterError("task", "cannot be nil")
// 	}
// 	if m == nil {
// 		return scerr.InvalidParameterError("f", "cannot be nil")
// 	}

// 	err = c.Lock(task)
// 	if err != nil {
// 		return err
// 	}
// 	defer func() {
// 		unlockErr := c.Unlock(task)
// 		if unlockErr != nil {
// 			logrus.Warn(unlockErr)
// 		}
// 		if err == nil && unlockErr != nil {
// 			err = unlockErr
// 		}
// 	}()

// 	c.foreman = f.(*foreman)
// 	return nil
// }

// Create creates the necessary infrastructure of the Cluster
func (c *controller) Create(task concurrency.Task, req cluster.Request) (err error) {
	if c == nil {
		return scerr.InvalidInstanceError()
	}
	defer scerr.OnPanic(&err)()

	if task == nil {
		task, err = concurrency.RootTask()
		if err != nil {
			return err
		}
	}

	tracer := concurrency.NewTracer(task, "", true).GoingIn()
	defer tracer.OnExitTrace()()
	defer temporal.NewStopwatch().OnExitLogInfo(
		fmt.Sprintf("Starting creation of infrastructure of cluster '%s'...", req.Name),
		fmt.Sprintf("Ending creation of infrastructure of cluster '%s'", req.Name),
	)()
	defer scerr.OnExitLogError(tracer.TraceMessage("failed to create cluster infrastructure:"), &err)()

	// VPL: Moved to resources.Cluster
	// // VPL: For now, always disable addition of feature proxycache-client
	// err = c.Properties.Alter(Property.FeaturesV1, func(clonable data.Clonable) error {
	// 	featuresV1, ok := clonable.(*propsv1.Features)
	// 	if !ok {
	// 		return scerr.InconsistentError("'*propsv1.Features' expected, '%s' provided", reflect.TypeOf(clonable).String())
	// 	}
	// 	featuresV1.Disabled["proxycache"] = struct{}{}
	// 	return nil
	// })
	// if err != nil {
	// 	logrus.Errorf("failed to disable feature 'proxycache': %v", err)
	// 	return err
	// }
	// // ENDVPL

	err = c.construct(task, req)
	return err
}

// Service returns the service from the provider
func (c *controller) Service(task concurrency.Task) (svc iaas.Service) {
	var err error
	defer scerr.OnExitLogError(concurrency.NewTracer(task, "", concurrency.IsLogActive("Trace.Controller")).TraceMessage(""), &err)()

	if c == nil {
		err = scerr.InvalidInstanceError()
		return nil
	}

	ignoredErr := c.RLock(task)
	if ignoredErr != nil {
		err = ignoredErr
		return nil
	}
	defer func() {
		unlockErr := c.RUnlock(task)
		if unlockErr != nil {
			logrus.Warn(unlockErr)
			svc = nil
		}
		if err == nil && unlockErr != nil {
			err = unlockErr
		}
	}()
	return c.service
}

// construct ...
func (c *controller) construct(task concurrency.Task, req Request) (err error) {
	defer scerr.OnPanic(&err)()

	tracer := concurrency.NewTracer(task, "", true).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	// Wants to inform about the duration of the operation
	defer temporal.NewStopwatch().OnExitLogInfo(
		fmt.Sprintf("Starting construction of cluster '%s'...", req.Name),
		fmt.Sprintf("Ending construction of cluster '%s'", req.Name),
	)()

	state := clusterstate.Unknown

	// defer func() {
	// 	if err != nil {
	// 		state = clusterstate.Error
	// 	} else {
	// 		state = clusterstate.Created
	// 	}

	// 	metaErr := c.Alter(task, func(clonable data.Clonable) error {
	// 		// Cluster created and configured successfully
	// 		return c.Properties(task).Alter(Property.StateV1, func(clonable interface{}) error {
	// 			stateV1, ok := clonable.(*clusterpropsv1.State)
	// 			if !ok {
	// 				return scerr.InconsistentError("'*propsv1.State' expected, '%s' provided", reflect.TypeOf(clonable).String())
	// 			}
	// 			stateV1.State = state
	// 			return nil
	// 		})
	// 	})
	// 	if metaErr != nil {
	// 		err = scerr.AddConsequence(err, metaErr)
	// 	}
	// }()

	if task == nil {
		task = concurrency.RootTask()
	}

	// Generate needed password for account cladm
	cladmPassword, err := utils.GeneratePassword(16)
	if err != nil {
		return err
	}

	// Determine default image
	var imageID string
	if req.NodesDef != nil {
		imageID = req.NodesDef.ImageId
	}
	if imageID == "" && c.makers.DefaultImage != nil {
		imageID = c.makers.DefaultImage(task, b)
	}
	if imageID == "" {
		imageID = "Ubuntu 18.04"
	}

	// Determine Gateway sizing
	var gatewaysDefault *resources.SizingRequirements
	if c.makers.DefaultGatewaySizing != nil {
		gatewaysDefault = complementSizingRequirements(nil, c.makers.DefaultGatewaySizing(task, c))
	} else {
		gatewaysDefault = resources.SizingRequirements{
			MinCores:    2,
			MaxCores:    4,
			MinRAMSize:  7.0,
			MaxRAMSize:  16.0,
			MinDiskSize: 50,
			MinGPU:      -1,
		}
	}
	gatewaysDefault.ImageId = imageID
	gatewaysDef := complementSizingRequirements(req.GatewaysDef, gatewaysDefault)

	// Determine master sizing
	var mastersDefault *resources.SizingRequirements
	if c.makers.DefaultMasterSizing != nil {
		mastersDefault = complementSizingRequirements(nil, c.makers.DefaultMasterSizing(task, c))
	} else {
		mastersDefault = resources.SizingRequirements{
			MinCores:    4,
			MaxCores:    8,
			MinRAMSize:  15.0,
			MaxRAMSize:  32.0,
			MinDiskSize: 100,
			MinGPU:      -1,
		}
	}
	// Note: no way yet to define master sizing from cli...
	mastersDefault.ImageId = imageID
	mastersDef := complementSizingRequirements(req.MastersDef, mastersDefault)

	// Determine node sizing
	var nodesDefault resources.SizingRequirements
	if c.makers.DefaultNodeSizing != nil {
		nodesDefault = complementSizingRequirements(nil, c.makers.DefaultNodeSizing(task, c))
	} else {
		nodesDefault = resources.SizingRequirements{
			MinCores:    4,
			MaxCores:    8,
			MinRAMSize:  15.0,
			MaxRAMSize:  32.0,
			MinDiskSize: 100,
			MinGPU:      -1,
		}
	}
	nodesDefault.ImageId = imageID
	nodesDef := complementSizingRequirements(req.NodesDef, nodesDefault)

	// Initialize service to use
	clientInstance := client.New()
	tenant, err := clientInstance.Tenant.Get(temporal.GetExecutionTimeout())
	if err != nil {
		return err
	}
	svc, err := iaas.UseService(tenant.Name)
	if err != nil {
		return err
	}

	// Determine if Gateway Failover must be set
	caps := svc.Capabilities()
	gwFailoverDisabled := req.Complexity == clustercomplexity.Small || !caps.PrivateVirtualIP
	for k := range req.DisabledDefaultFeatures {
		if k == "gateway-failover" {
			gwFailoverDisabled = true
			break
		}
	}

	// Creates network
	logrus.Debugf("[cluster %s] creating network 'net-%s'", req.Name, req.Name)
	req.Name = strings.ToLower(req.Name)
	networkName := "net-" + req.Name
	sizing := srvutils.FromHostDefinitionToGatewayDefinition(gatewaysDef)
	def := resources.NetworkDefinition{
		Name:     networkName,
		Cidr:     req.CIDR,
		Gateway:  &sizing,
		FailOver: !gwFailoverDisabled,
	}
	clientNetwork := clientInstance.Network
	network, err := clientNetwork.Create(def, temporal.GetExecutionTimeout())
	if err != nil {
		return err
	}
	logrus.Debugf("[cluster %s] network '%s' creation successful.", req.Name, networkName)
	req.NetworkID = network.Id

	defer func() {
		if err != nil && !req.KeepOnFailure {
			derr := clientNetwork.Delete([]string{network.Id}, temporal.GetExecutionTimeout())
			if derr != nil {
				err = scerr.AddConsequence(err, derr)
			}
		}
	}()

	// Saving Cluster parameters, with status 'Creating'
	var (
		kp                               *resources.KeyPair
		kpName                           string
		primaryGateway, secondaryGateway *resources.Host
	)

	// Loads primary gateway metadata
	// primaryGatewayMetadata, err := providermetadata.LoadHost(svc, network.GatewayId)
	// if err != nil {
	// 	if _, ok := err.(*scerr.ErrNotFound); ok {
	// 		if !ok {
	// 			return err
	// 		}
	// 	}
	// 	return err
	// }
	// primaryGateway, _, _, err = svc.InspectHost(network.GatewayId)
	// if err != nil {
	// 	return err
	// }
	err = clientInstance.SSH.WaitReady(network.GatewayId, temporal.GetExecutionTimeout())
	if err != nil {
		return client.DecorateError(err, "wait for remote ssh service to be ready", false)
	}

	// Loads secondary gateway metadata
	if !gwFailoverDisabled {
		// secondaryGatewayMetadata, err := providermetadata.LoadHost(svc, network.SecondaryGatewayId)
		// if err != nil {
		// 	if _, ok := err.(*scerr.ErrNotFound); ok {
		// 		if !ok {
		// 			return err
		// 		}
		// 	}
		// 	return err
		// }
		// secondaryGateway, err = secondaryGatewayMetadata.Get()
		// if err != nil {
		// 	return err
		// }
		err = clientInstance.SSH.WaitReady(network.SecondaryGatewayId, temporal.GetExecutionTimeout())
		if err != nil {
			return client.DecorateError(err, "wait for remote ssh service to be ready", false)
		}
	}

	// Create a KeyPair for the user cladm
	kpName = "cluster_" + req.Name + "_cladm_key"
	kp, err = svc.CreateKeyPair(kpName)
	if err != nil {
		return err
	}

	// Saving Cluster metadata, with status 'Creating'
	c.Name = req.Name
	c.Flavor = req.Flavor
	c.Complexity = req.Complexity
	c.Keypair = kp
	c.AdminPassword = cladmPassword
	// VPL: moved to resources.Cluster
	// err = b.cluster.UpdateMetadata(task, func() error {
	// 	err := b.cluster.GetProperties(task).Alter(Property.DefaultsV2, func(v interface{}) error {
	// 		defaultsV2 := v.(*clusterpropertiesv2.Defaults)
	// 		defaultsV2.GatewaySizing = srvutils.FromProtocolHostSizing(*gatewaysDef.Sizing)
	// 		defaultsV2.MasterSizing = srvutils.FromProtocolHostSizing(*mastersDef.Sizing)
	// 		defaultsV2.NodeSizing = srvutils.FromProtocolHostSizing(*nodesDef.Sizing)
	// 		defaultsV2.Image = imageID
	// 		return nil
	// 	})
	// 	if err != nil {
	// 		return err
	// 	}

	// 	err = b.cluster.GetProperties(task).Alter(Property.StateV1, func(v interface{}) error {
	// 		v.(*clusterpropsv1.State).State = clusterstate.Creating
	// 		return nil
	// 	})
	// 	if err != nil {
	// 		return err
	// 	}

	// 	err = b.cluster.GetProperties(task).Alter(Property.CompositeV1, func(v interface{}) error {
	// 		v.(*clusterpropsv1.Composite).Tenants = []string{req.Tenant}
	// 		return nil
	// 	})
	// 	if err != nil {
	// 		return err
	// 	}

	// 	return b.cluster.GetProperties(task).Alter(Property.NetworkV2, func(v interface{}) error {
	// 		networkV2 := v.(*clusterpropertiesv2.Network)
	// 		networkV2.NetworkID = req.NetworkID
	// 		networkV2.CIDR = req.CIDR
	// 		networkV2.GatewayID = primaryGateway.ID
	// 		networkV2.GatewayIP = primaryGateway.GetPrivateIP()
	// 		if !gwFailoverDisabled {
	// 			networkV2.SecondaryGatewayID = secondaryGateway.ID
	// 			networkV2.SecondaryGatewayIP = secondaryGateway.GetPrivateIP()
	// 			networkV2.DefaultRouteIP = network.VirtualIp.PrivateIp
	// 			// VPL: no public IP on VIP yet...
	// 			// networkV2.EndpointIP = network.VirtualIp.PublicIp
	// 			networkV2.EndpointIP = primaryGateway.GetPublicIP()
	// 			networkV2.PrimaryPublicIP = primaryGateway.GetPublicIP()
	// 			networkV2.SecondaryPublicIP = secondaryGateway.GetPublicIP()
	// 		} else {
	// 			networkV2.DefaultRouteIP = primaryGateway.GetPrivateIP()
	// 			networkV2.EndpointIP = primaryGateway.GetPublicIP()
	// 			networkV2.PrimaryPublicIP = networkV2.EndpointIP
	// 		}
	// 		return nil
	// 	})
	// })
	// if err != nil {
	// 	return err
	// }

	defer func() {
		if err != nil && !req.KeepOnFailure {
			derr := b.cluster.DeleteMetadata(task)
			if derr != nil {
				err = scerr.AddConsequence(err, derr)
			}
		}
	}()

	masterCount, privateNodeCount, _ := c.determineRequiredNodes(task)
	var (
		primaryGatewayStatus   error
		secondaryGatewayStatus error
		mastersStatus          error
		privateNodesStatus     error
		secondaryGatewayTask   concurrency.Task
	)

	// Step 1: starts gateway installation plus masters creation plus nodes creation
	primaryGatewayTask, err := task.StartInSubTask(b.taskInstallGateway, srvutils.ToPBHost(primaryGateway))
	if err != nil {
		return err
	}
	if !gwFailoverDisabled {
		secondaryGatewayTask, err = task.StartInSubTask(b.taskInstallGateway, srvutils.ToPBHost(secondaryGateway))
		if err != nil {
			return err
		}
	}
	mastersTask, err := task.StartInSubTask(b.taskCreateMasters, data.Map{
		"count":     masterCount,
		"masterDef": mastersDef,
		"nokeep":    !req.KeepOnFailure,
	})
	if err != nil {
		return err
	}

	privateNodesTask, err := task.StartInSubTask(b.taskCreateNodes, data.Map{
		"count":   privateNodeCount,
		"public":  false,
		"nodeDef": nodesDef,
		"nokeep":  !req.KeepOnFailure,
	})
	if err != nil {
		return err
	}

	// Step 2: awaits gateway installation end and masters installation end
	_, primaryGatewayStatus = primaryGatewayTask.Wait()
	if primaryGatewayStatus != nil {
		abortMasterErr := mastersTask.Abort()
		if abortMasterErr != nil {
			primaryGatewayStatus = scerr.AddConsequence(primaryGatewayStatus, abortMasterErr)
		}
		abortNodesErr := privateNodesTask.Abort()
		if abortNodesErr != nil {
			primaryGatewayStatus = scerr.AddConsequence(primaryGatewayStatus, abortNodesErr)
		}
		return primaryGatewayStatus
	}
	if !gwFailoverDisabled {
		if secondaryGatewayTask != nil {
			_, secondaryGatewayStatus = secondaryGatewayTask.Wait()
			if secondaryGatewayStatus != nil {
				abortMasterErr := mastersTask.Abort()
				if abortMasterErr != nil {
					secondaryGatewayStatus = scerr.AddConsequence(secondaryGatewayStatus, abortMasterErr)
				}
				abortNodesErr := privateNodesTask.Abort()
				if abortNodesErr != nil {
					secondaryGatewayStatus = scerr.AddConsequence(secondaryGatewayStatus, abortNodesErr)
				}
				return secondaryGatewayStatus
			}
		}
	}

	// Starting from here, delete masters if exiting with error and req.KeepOnFailure is not true
	defer func() {
		if err != nil && !req.KeepOnFailure {
			list, merr := b.cluster.ListMasterIDs(task)
			if merr != nil {
				err = scerr.AddConsequence(err, merr)
			} else {
				values := make([]string, 0, len(list))
				for _, v := range list {
					values = append(values, v)
				}
				derr := client.New().Host.Delete(values, temporal.GetExecutionTimeout())
				if derr != nil {
					err = scerr.AddConsequence(err, derr)
				}
			}
		}
	}()
	_, mastersStatus = mastersTask.Wait()
	if mastersStatus != nil {
		abortNodesErr := privateNodesTask.Abort()
		if abortNodesErr != nil {
			mastersStatus = scerr.AddConsequence(mastersStatus, abortNodesErr)
		}
		return mastersStatus
	}

	// Step 3: run (not start so no parallelism here) gateway configuration (needs MasterIPs so masters must be installed first)
	// Configure Gateway(s) and waits for the result
	primaryGatewayTask, err = task.StartInSubTask(b.taskConfigureGateway, srvutils.ToPBHost(primaryGateway))
	if err != nil {
		return err
	}
	if !gwFailoverDisabled {
		secondaryGatewayTask, err = task.StartInSubTask(b.taskConfigureGateway, srvutils.ToPBHost(secondaryGateway))
		if err != nil {
			return err
		}
	}
	_, primaryGatewayStatus = primaryGatewayTask.Wait()
	if primaryGatewayStatus != nil {
		if !gwFailoverDisabled {
			if secondaryGatewayTask != nil {
				secondaryGatewayErr := secondaryGatewayTask.Abort()
				if secondaryGatewayErr != nil {
					primaryGatewayStatus = scerr.AddConsequence(primaryGatewayStatus, secondaryGatewayErr)
				}
			}
		}
		return primaryGatewayStatus
	}

	if !gwFailoverDisabled {
		if secondaryGatewayTask != nil {
			_, secondaryGatewayStatus = secondaryGatewayTask.Wait()
			if secondaryGatewayStatus != nil {
				return secondaryGatewayStatus
			}
		}
	}

	// Step 4: configure masters (if masters created successfully and gateway configure successfully)
	_, mastersStatus = task.RunInSubTask(b.taskConfigureMasters, nil)
	if mastersStatus != nil {
		return mastersStatus
	}

	// Starting from here, delete nodes on failure if exits with error and req.KeepOnFailure is false
	defer func() {
		if err != nil && !req.KeepOnFailure {
			clientHost := clientInstance.Host
			list, lerr := b.cluster.ListNodeIDs(task)
			if lerr != nil {
				err = scerr.AddConsequence(err, lerr)
			} else {
				derr := clientHost.Delete(list.Values(), temporal.GetExecutionTimeout())
				if derr != nil {
					err = scerr.AddConsequence(err, derr)
				}
			}
		}
	}()

	// Step 5: awaits nodes creation
	_, privateNodesStatus = privateNodesTask.Wait()
	if privateNodesStatus != nil {
		return privateNodesStatus
	}

	// Step 6: Starts nodes configuration, if all masters and nodes
	// have been created and gateway has been configured with success
	_, privateNodesStatus = task.RunInSubTask(b.taskConfigureNodes, nil)
	if privateNodesStatus != nil {
		return privateNodesStatus
	}

	// At the end, configure cluster as a whole
	err = c.configureCluster(task, data.Map{
		"PrimaryGateway":   primaryGateway,
		"SecondaryGateway": secondaryGateway,
	})
	if err != nil {
		return err
	}

	return nil
}

// complementSizingRequirements complements req with default values if needed
func complementSizingRequirements(req *resources.SizingRequirements, def resources.SizingRequirements) *resources.SizingRequirements {
	var finalDef resources.SizingRequirements
	if req == nil {
		finalDef = def
	} else {
		finalDef = *req

		if def.MinCores > 0 && finalDef.MinCores == 0 {
			finalDef.MinCores = def.MinCores
		}
		if def.MaxCores > 0 && finalDef.MaxCores == 0 {
			finalDef.MaxCores = def.MaxCores
		}
		if def.MinRAMSize > 0.0 && finalDef.MinRAMSize == 0.0 {
			finalDef.MinRAMSize = def.MinRAMSize
		}
		if def.MaxRAMSize > 0.0 && finalDef.MaxRAMSize == 0.0 {
			finalDef.MaxRAMSize = def.MaxRAMSize
		}
		if def.MinDiskSize > 0 && finalDef.MinDiskSize == 0 {
			finalDef.MinDiskSize = def.MinDiskSize
		}
		if finalDef.MinGPU <= 0 && def.MinGPU > 0 {
			finalDef.MinGPU = def.MinGPU
		}
		if finalDef.MinCpuFreq == 0 && def.MinCpuFreq > 0 {
			finalDef.MinCpuFreq = def.MinCpuFreq
		}
		if finalDef.ImageId == "" {
			finalDef.ImageId = def.ImageId
		}

		if finalDef.MinCores <= 0 {
			finalDef.MinCores = 2
		}
		if finalDef.MaxCores <= 0 {
			finalDef.MaxCores = 4
		}
		if finalDef.MinRAMSize <= 0.0 {
			finalDef.MinRAMSize = 7.0
		}
		if finalDef.MaxRAMmSize <= 0.0 {
			finalDef.MaxRAMSize = 16.0
		}
		if finalDef.MinDiskSize <= 0 {
			finalDef.MinDiskSize = 50
		}
	}

	return &finalDef
}

// Identity returns the core data of a cluster
func (c *controller) Identity(task concurrency.Task) (id resources.ClusterIdentity, err error) {
	if task == nil {
		task, err = concurrency.RootTask()
		if err != nil {
			return err
		}
	}

	defer scerr.OnExitLogError(concurrency.NewTracer(task, "", concurrency.IsLogActive("Trace.Controller")).TraceMessage(""), &err)()

	if c == nil {
		err = scerr.InvalidInstanceError()
		return resources.ClusterIdentity{}
	}

	ignoredErr := c.RLock(task)
	if ignoredErr != nil {
		err = ignoredErr
		return resources.ClusterIdentity{}
	}
	defer func() {
		unlockErr := c.RUnlock(task)
		if unlockErr != nil {
			logrus.Warn(unlockErr)
			id = identity.ClusterIdentity{}
		}
		if err == nil && unlockErr != nil {
			err = unlockErr
		}
	}()
	return c.Identity
}

//VPL: moved to resources.Cluster
// // GetProperties returns the properties of the cluster
// func (c *Controller) GetProperties(task concurrency.Task) (props *serialize.JSONProperties) {
// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	var err error
// 	defer scerr.OnExitLogError(concurrency.NewTracer(task, "", concurrency.IsLogActive("Trace.Controller")).TraceMessage(""), &err)()

// 	if c == nil {
// 		err = scerr.InvalidInstanceError()
// 		return nil
// 	}

// 	ignoredErr := c.RLock(task)
// 	if ignoredErr != nil {
// 		err = ignoredErr
// 		return nil
// 	}
// 	defer func() {
// 		unlockErr := c.RUnlock(task)
// 		if unlockErr != nil {
// 			logrus.Warn(unlockErr)
// 			props = nil
// 		}
// 		if err == nil && unlockErr != nil {
// 			err = unlockErr
// 		}
// 	}()

// 	return c.Properties
// }

//VPL: Moved to resources.Cluster
// // GetNetworkConfig returns the network configuration of the cluster
// func (c *Controller) GetNetworkConfig(task concurrency.Task) (_ propertiesv2.Network, err error) {
// 	defer scerr.OnPanic(&err)()

// 	config := propertiesv2.Network{}
// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	defer scerr.OnExitLogError(concurrency.NewTracer(task, "", concurrency.IsLogActive("Trace.Controller")).TraceMessage(""), &err)()

// 	if c == nil {
// 		return config, scerr.InvalidInstanceError()
// 	}

// 	c.RLock(task)
// 	if c.Properties.Lookup(Property.NetworkV2) {
// 		_ = c.Properties.Inspect(Property.NetworkV2, func(v data.Clonable) error {
// 			config = *(v.(*propertiesv2.Network))
// 			return nil
// 		})
// 	} else {
// 		_ = c.Properties.Inspect(Property.NetworkV1, func(v data.Clonable) error {
// 			networkV1 := v.(*propsv1.Network)
// 			config = propertiesv2.Network{
// 				NetworkID:      networkV1.NetworkID,
// 				CIDR:           networkV1.CIDR,
// 				GatewayID:      networkV1.GatewayID,
// 				GatewayIP:      networkV1.GatewayIP,
// 				DefaultRouteIP: networkV1.GatewayIP,
// 				EndpointIP:     networkV1.PublicIP,
// 			}
// 			return nil
// 		})
// 	}

// 	return config, nil
// }

//VPL: moved to resources.Cluster
// // CountNodes returns the number of nodes in the cluster
// func (c *Controller) CountNodes(task concurrency.Task) (_ uint, err error) {
// 	if c == nil {
// 		return 0, scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	defer scerr.OnExitLogError(concurrency.NewTracer(task, "", concurrency.IsLogActive("Trace.Controller")).TraceMessage(""), &err)()

// 	var count uint

// 	c.RLock(task)
// 	err = c.GetProperties(task).Inspect(Property.NodesV2, func(v data.Clonable) error {
// 		count = uint(len(v.(*propertiesv2.Nodes).PrivateNodes))
// 		return nil
// 	})
// 	if err != nil {
// 		logrus.Debugf("failed to count nodes: %v", err)
// 		return count, err
// 	}
// 	return count, err
// }

// VPL: Moved to resources.Cluster
// // ListMasters lists the names of the master nodes in the Cluster
// func (c *Controller) ListMasters(task concurrency.Task) (_ []*propertiesv2.Node, err error) {
// 	if c == nil {
// 		return nil, scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	var list []*propertiesv2.Node
// 	err = c.Properties.Inspect(Property.NodesV2, func(clonable data.Clonable) error {
// 		nodesV2, ok := clonable.(*propertiesv2.Nodes)
// 		if !ok {
// 			return scerr.InconsistentError("'*propertiesv2.Nodes' expected, '%s' provided", reflect.TypeOf(clonable).String())
// 		}
// 		list = nodesV2.Masters
// 		return nil
// 	})
// 	if err != nil {
// 		logrus.Errorf("failed to get list of master names: %v", err)
// 		return list, err
// 	}
// 	return list, err
// }

// VPL: Moved to resources.Cluster
// // ListMasterNames lists the names of the master nodes in the Cluster
// func (c *Controller) ListMasterNames(task concurrency.Task) (nodelist cluster.NodeList, err error) {
// 	defer scerr.OnPanic(&err)()
// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	list := cluster.NodeList{}
// 	err = c.Properties.Inspect(Property.NodesV2, func(v data.Clonable) error {
// 		nodesV2 := v.(*propertiesv2.Nodes).Masters
// 		for _, v := range nodesV2 {
// 			list[v.NumericalID] = v.Name
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		// logrus.Errorf("failed to get list of master names: %v", err)
// 		return nil, err
// 	}
// 	return list, nil
// }

// VPL: Moved to resources.Cluster
// // ListMasterIDs lists the IDs of the master nodes in the Cluster
// func (c *Controller) ListMasterIDs(task concurrency.Task) (_ cluster.NodeList, err error) {
// 	if c == nil {
// 		return scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	list := cluster.NodeList{}
// 	err = c.Properties.Inspect(Property.NodesV2, func(v data.Clonable) error {
// 		nodesV2 := v.(*propertiesv2.Nodes).Masters
// 		for _, v := range nodesV2 {
// 			list[v.NumericalID] = v.ID
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to get list of master IDs: %v", err)
// 	}

// 	return list, nil
// }

// VPL: Moved to resources.Cluster
// // ListMasterIPs lists the IP addresses of the master nodes in the Cluster
// func (c *Controller) ListMasterIPs(task concurrency.Task) (_ cluster.NodeList, err error) {
// 	defer scerr.OnPanic(&err)()
// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	list := cluster.NodeList{}
// 	err = c.Properties.Inspect(Property.NodesV2, func(v data.Clonable) error {
// 		nodesV2 := v.(*propertiesv2.Nodes).Masters
// 		for _, v := range nodesV2 {
// 			list[v.NumericalID] = v.PrivateIP
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		logrus.Errorf("failed to get list of master IPs: %v", err)
// 		return nil, err
// 	}
// 	return list, err
// }

//VPL: Moved to resources.Cluster
// // ListNodes lists the nodes in the Cluster
// func (c *Controller) ListNodes(task concurrency.Task) (nodelist []*propertiesv2.Node, err error) {
// 	defer scerr.OnPanic(&err)()
// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	var list []*propertiesv2.Node
// 	err = c.Properties.Inspect(Property.NodesV2, func(v data.Clonable) error {
// 		list = v.(*propertiesv2.Nodes).PrivateNodes
// 		return nil
// 	})
// 	if err != nil {
// 		// logrus.Errorf("failed to get list of node IDs: %v", err)
// 		return nil, err
// 	}
// 	return list, nil
// }

// VPL: Moved to resources.Cluster
// // ListNodeNames lists the names of the nodes in the Cluster
// func (c *Controller) ListNodeNames(task concurrency.Task) (nodelist cluster.NodeList, err error) {
// 	defer scerr.OnPanic(&err)()
// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	list := cluster.NodeList{}
// 	err := c.Properties.Inspect(Property.NodesV2, func(v data.Clonable) error {
// 		nodesV2 := v.(*propertiesv2.Nodes).PrivateNodes
// 		for _, v := range nodesV2 {
// 			list[v.NumericalID] = v.Name
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		// logrus.Errorf("failed to get list of node IDs: %v", err)
// 		return nil, err
// 	}
// 	return list, err
// }

// VPL: moved to resources.CLuster
// // ListNodeIDs lists the IDs of the nodes in the Cluster
// func (c *Controller) ListNodeIDs(task concurrency.Task) (nodelist cluster.NodeList, err error) {
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	list := cluster.NodeList{}
// 	err := c.Properties.Inspect(Property.NodesV2, func(v data.Clonable) error {
// 		nodesV2 := v.(*propertiesv2.Nodes).PrivateNodes
// 		for _, v := range nodesV2 {
// 			list[v.NumericalID] = v.ID
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		// logrus.Errorf("failed to get list of node IDs: %v", err)
// 		return nil, err
// 	}
// 	return list, err
// }

// VPL: moved to resources.Cluster
// // ListNodeIPs lists the IP addresses of the nodes in the Cluster
// func (c *Controller) ListNodeIPs(task concurrency.Task) (nodelist cluster.NodeList, err error) {
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	var list cluster.NodeList
// 	err := c.Properties.Inspect(Property.NodesV2, func(v data.Clonable) error {
// 		nodesV2 := v.(*propertiesv2.Nodes).PrivateNodes
// 		for _, v := range nodesV2 {
// 			list[v.NumericalID] = v.PrivateIP
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		// logrus.Errorf("failed to get list of node IP addresses: %v", err)
// 		return nil, err
// 	}
// 	return list, nil
// }

// VPL: moved to resources.Cluster
// // GetNode returns a node based on its ID
// func (c *Controller) GetNode(task concurrency.Task, hostID string) (host *pb.Host, err error) {
// 	if c == nil {
// 		return nil, scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if hostID == "" {
// 		return nil, scerr.InvalidParameterError("hostID", "cannot be empty string")
// 	}
// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	tracer := concurrency.NewTracer(task, fmt.Sprintf("(%s)", hostID), true)
// 	defer tracer.GoingIn().OnExitTrace()()
// 	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

// 	found := false
// 	err = c.Properties.Inspect(Property.NodesV2, func(v data.Clonable) error {
// 		nodesV2 := v.(*propertiesv2.Nodes)
// 		found, _ = contains(nodesV2.PrivateNodes, hostID)
// 		return nil
// 	})
// 	if err != nil {
// 		return nil, err
// 	}
// 	if !found {
// 		return nil, fmt.Errorf("failed to find node '%s' in Cluster '%s'", hostID, c.Name)
// 	}
// 	return client.New().Host.Inspect(hostID, temporal.GetExecutionTimeout())
// }

// VPL: moved to resources.Cluster and renamed to LookupNode
// // SearchNode tells if an host ID corresponds to a node of the Cluster
// func (c *Controller) SearchNode(task concurrency.Task, hostID string) (found bool, err error) {
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	found := false
// 	_ = c.Properties.Inspect(Property.NodesV2, func(v data.Clonable) error {
// 		found, _ = contains(v.(*propertiesv2.Nodes).PrivateNodes, hostID)
// 		return nil
// 	})

// 	return found, err
// }

// VPL: moved to resources.Cluster
// // FindAvailableMaster returns the *propertiesv2.Node corresponding to the first available master for execution
// func (c *Controller) FindAvailableMaster(task concurrency.Task) (_ *propertiesv2.Node, err error) {
// 	if c == nil {
// 		return nil, scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	tracer := concurrency.NewTracer(task, "", true).GoingIn()
// 	defer tracer.OnExitTrace()()
// 	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

// 	found := false
// 	clientHost := client.New().Host
// 	masters, err := c.ListMasters(task)
// 	if err != nil {
// 		return nil, err
// 	}

// 	var (
// 		lastError error
// 		master    *propertiesv2.Node
// 	)
// 	for _, master = range masters {
// 		sshCfg, err := clientHost.SSHConfig(master.ID)
// 		if err != nil {
// 			lastError = err
// 			logrus.Errorf("failed to get ssh config for master '%s': %s", master.ID, err.Error())
// 			continue
// 		}

// 		ctx, err := task.GetContext()
// 		if err != nil {
// 			logrus.Errorf("failed to get context: %s", err.Error())
// 			continue
// 		}

// 		_, err = sshCfg.WaitServerReady(ctx, "ready", temporal.GetConnectSSHTimeout())
// 		if err != nil {
// 			if _, ok := err.(retry.ErrTimeout); ok {
// 				lastError = err
// 				continue
// 			}
// 			return nil, err
// 		}
// 		found = true
// 		break
// 	}
// 	if !found {
// 		return nil, fmt.Errorf("failed to find available master: %v", lastError)
// 	}
// 	return master, nil
// }

// VPL: moved in resources.Cluster
// // FindAvailableNode returns the propertiesv2.Node corresponding to first available node
// func (c *Controller) FindAvailableNode(task concurrency.Task) (_ *propertiesv2.Node, err error) {
// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	defer scerr.OnPanic(&err)()

// 	tracer := concurrency.NewTracer(task, "", true).GoingIn()
// 	defer tracer.OnExitTrace()()
// 	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

// 	clientHost := client.New().Host
// 	list, err := c.ListNodes(task)
// 	if err != nil {
// 		return nil, err
// 	}

// 	found := false
// 	var node *propertiesv2.Node
// 	for _, node = range list {
// 		sshCfg, err := clientHost.SSHConfig(node.ID)
// 		if err != nil {
// 			logrus.Errorf("failed to get ssh config of node '%s': %s", node.ID, err.Error())
// 			continue
// 		}

// 		ctx, err := task.Context()
// 		if err != nil {
// 			logrus.Errorf("failed to get context: %s", err.Error())
// 			continue
// 		}

// 		_, err = sshCfg.WaitServerReady(ctx, "ready", temporal.GetConnectSSHTimeout())
// 		if err != nil {
// 			if _, ok := err.(retry.ErrTimeout); ok {
// 				continue
// 			}
// 			return nil, err
// 		}
// 		found = true
// 		break
// 	}
// 	if !found {
// 		return nil, fmt.Errorf("failed to find available node")
// 	}
// 	return node, nil
// }

// VPL: moved in resources.Cluster
// // UpdateMetadata writes Cluster config in Object Storage
// func (c *Controller) UpdateMetadata(task concurrency.Task, updatefn func() error) (err error) {
// 	if c == nil {
// 		return scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	tracer := concurrency.NewTracer(task, "", true).WithStopwatch().GoingIn()
// 	defer tracer.OnExitTrace()()
// 	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

// 	err = c.Lock(task)
// 	if err != nil {
// 		return err
// 	}
// 	defer func() {
// 		unlockErr := c.Unlock(task)
// 		if unlockErr != nil {
// 			logrus.Warn(unlockErr)
// 		}
// 		if err == nil && unlockErr != nil {
// 			err = unlockErr
// 		}
// 	}()

// 	c.metadata.Acquire()
// 	defer c.metadata.Release()

// 	err = c.metadata.Reload(task)
// 	if err != nil {
// 		return err
// 	}
// 	if c.metadata.Written() {
// 		mc, err := c.metadata.Get()
// 		if err != nil {
// 			return err
// 		}
// 		err = c.replace(task, mc)
// 		if err != nil {
// 			return err
// 		}
// 	} else {
// 		c.metadata.Carry(task, c)
// 	}

// 	if updatefn != nil {
// 		err := updatefn()
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return c.metadata.Write()
// }

// VPL: moved in resources.Cluster
// // DeleteMetadata removes Cluster metadata from Object Storage
// func (c *Controller) DeleteMetadata(task concurrency.Task) (err error) {
// 	if c == nil {
// 		return scerr.InvalidInstanceError()
// 	}

// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	tracer := concurrency.NewTracer(task, "", true).WithStopwatch().GoingIn()
// 	defer tracer.OnExitTrace()()
// 	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

// 	err = c.Lock(task)
// 	if err != nil {
// 		return err
// 	}
// 	defer func() {
// 		unlockErr := c.Unlock(task)
// 		if unlockErr != nil {
// 			logrus.Warn(unlockErr)
// 		}
// 		if err == nil && unlockErr != nil {
// 			err = unlockErr
// 		}
// 	}()

// 	c.metadata.Acquire()
// 	defer c.metadata.Release()

// 	return c.metadata.Delete()
// }

// VPL: moved in resources.Cluster
// func contains(list []*propertiesv2.Node, hostID string) (bool, int) {
// 	var idx int
// 	found := false
// 	for i, v := range list {
// 		if v.ID == hostID {
// 			found = true
// 			idx = i
// 			break
// 		}
// 	}
// 	return found, idx
// }

// Serialize converts cluster data to JSON
func (c *controller) Serialize() ([]byte, error) {
	return serialize.ToJSON(c)
}

// Deserialize reads json code and reinstantiates cluster
func (c *controller) Deserialize(buf []byte) error {
	return serialize.FromJSON(buf, c)
}

// VPL: moved to resources.Cluster
// // AddNode adds one node
// func (c *Controller) AddNode(task concurrency.Task, req *pb.HostDefinition) (string, error) {
// 	// No log enforcement here, delegated to AddNodes()

// 	hosts, err := c.AddNodes(task, 1, req)
// 	if err != nil {
// 		return "", err
// 	}
// 	return hosts[0], nil
// }

// // AddNodes adds <count> nodes
// func (c *Controller) AddNodes(task concurrency.Task, count uint, req *pb.HostDefinition) (hosts []string, err error) {
// 	if c == nil {
// 		return nil, scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if count == 0 {
// 		return nil, scerr.InvalidParameterError("count", "must be greater than zero")
// 	}
// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	tracer := concurrency.NewTracer(task, fmt.Sprintf("(%d)", count), true)
// 	defer tracer.GoingIn().OnExitTrace()()
// 	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

// 	nodeDef := complementHostDefinition(req, pb.HostDefinition{})
// 	var hostImage string

// 	// VPL: moved in resources.Cluster.AddNodes()
// 	// properties := c.GetProperties(concurrency.RootTask())
// 	// if !properties.Lookup(Property.DefaultsV2) {
// 	// 	err := properties.Inspect(Property.DefaultsV1, func(v interface{}) error {
// 	// 		defaultsV1 := v.(*propsv1.Defaults)
// 	// 		return c.UpdateMetadata(task, func() error {
// 	// 			return properties.Alter(Property.DefaultsV2, func(v interface{}) error {
// 	// 				defaultsV2 := v.(*propertiesv2.Defaults)
// 	// 				convertDefaultsV1ToDefaultsV2(defaultsV1, defaultsV2)
// 	// 				return nil
// 	// 			})
// 	// 		})
// 	// 	})
// 	// 	if err != nil {
// 	// 		return nil, err
// 	// 	}
// 	// }
// 	// err = properties.Inspect(Property.DefaultsV2, func(v interface{}) error {
// 	// 	defaultsV2 := v.(*propertiesv2.Defaults)
// 	// 	sizing := srvutils.ToPBHostSizing(defaultsV2.NodeSizing)
// 	// 	nodeDef.Sizing = &sizing
// 	// 	hostImage = defaultsV2.Image
// 	// 	return nil
// 	// })

// 	// if err != nil {
// 	// 	return nil, err
// 	// }

// 	if nodeDef.ImageId == "" {
// 		nodeDef.ImageId = hostImage
// 	}

// 	var (
// 		nodeType    NodeType.Enum
// 		nodeTypeStr string
// 		errors      []string
// 	)
// 	netCfg, err := c.GetNetworkConfig(task)
// 	if err != nil {
// 		return nil, err
// 	}
// 	nodeDef.Network = netCfg.NetworkID

// 	timeout := temporal.GetExecutionTimeout() + time.Duration(count)*time.Minute

// 	var subtasks []concurrency.Task
// 	for i := uint(0); i < count; i++ {
// 		subtask, err := task.StartInSubTask(c.foreman.taskCreateNode, data.Map{
// 			"index":   i + 1,
// 			"type":    nodeType,
// 			"nodeDef": nodeDef,
// 			"timeout": timeout,
// 		})
// 		if err != nil {
// 			return nil, err
// 		}
// 		subtasks = append(subtasks, subtask)
// 	}
// 	for _, s := range subtasks {
// 		result, err := s.Wait()
// 		if err != nil {
// 			errors = append(errors, err.Error())
// 		} else {
// 			hostName, ok := result.(string)
// 			if ok {
// 				if hostName != "" {
// 					hosts = append(hosts, hostName)
// 				}
// 			}
// 		}
// 	}
// 	hostClt := client.New().Host

// 	// Starting from here, delete nodes if exiting with error
// 	newHosts := hosts
// 	defer func() {
// 		if err != nil {
// 			if len(newHosts) > 0 {
// 				derr := hostClt.Delete(newHosts, temporal.GetExecutionTimeout())
// 				if derr != nil {
// 					logrus.Errorf("failed to delete nodes after failure to expand cluster")
// 				}
// 				err = scerr.AddConsequence(err, derr)
// 			}
// 		}
// 	}()

// 	if len(errors) > 0 {
// 		err = fmt.Errorf("errors occurred on %s node%s addition: %s", nodeTypeStr, utils.Plural(uint(len(errors))), strings.Join(errors, "\n"))
// 		return nil, err
// 	}

// 	// Now configure new nodes
// 	err = c.foreman.configureNodesFromList(task, hosts)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// At last join nodes to cluster
// 	err = c.foreman.joinNodesFromList(task, hosts)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return hosts, nil
// }

// VPL: moved in resources.Cluster
// func convertDefaultsV1ToDefaultsV2(defaultsV1 *propsv1.Defaults, defaultsV2 *propertiesv2.Defaults) {
// 	defaultsV2.Image = defaultsV1.Image
// 	defaultsV2.MasterSizing = resources.SizingRequirements{
// 		MinCores:    defaultsV1.MasterSizing.Cores,
// 		MinFreq:     defaultsV1.MasterSizing.CPUFreq,
// 		MinGPU:      defaultsV1.MasterSizing.GPUNumber,
// 		MinRAMSize:  defaultsV1.MasterSizing.RAMSize,
// 		MinDiskSize: defaultsV1.MasterSizing.DiskSize,
// 		Replaceable: defaultsV1.MasterSizing.Replaceable,
// 	}
// 	defaultsV2.NodeSizing = resources.SizingRequirements{
// 		MinCores:    defaultsV1.NodeSizing.Cores,
// 		MinFreq:     defaultsV1.NodeSizing.CPUFreq,
// 		MinGPU:      defaultsV1.NodeSizing.GPUNumber,
// 		MinRAMSize:  defaultsV1.NodeSizing.RAMSize,
// 		MinDiskSize: defaultsV1.NodeSizing.DiskSize,
// 		Replaceable: defaultsV1.NodeSizing.Replaceable,
// 	}
// }

// State returns the current state of the Cluster
// Uses the "maker" GetState from Foreman
func (c *controller) State(task concurrency.Task) (state clusterstate.Enum, err error) {
	if c == nil {
		return clusterstate.Unknown, scerr.InvalidInstanceError()
	}
	defer scerr.OnPanic(&err)()

	if task == nil {
		task = concurrency.RootTask()
	}

	tracer := concurrency.NewTracer(task, "", true).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	state, err = c.foreman.getState(task)
	if err != nil {
		return clusterstate.Unknown, err
	}

	// VPL: moved in resources.Cluster
	// err = c.UpdateMetadata(task, func() error {
	// 	return c.Properties.Alter(Property.StateV1, func(v interface{}) error {
	// 		stateV1 := v.(*propsv1.State)
	// 		stateV1.State = state
	// 		c.lastStateCollection = time.Now()
	// 		return nil
	// 	})
	// })
	return state, err
}

// VPL: moved in resources.Cluster
// // deleteMaster deletes the master specified by its ID
// func (c *Controller) deleteMaster(task concurrency.Task, hostID string) (err error) {
// 	if c == nil {
// 		return scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if hostID == "" {
// 		return scerr.InvalidParameterError("hostID", "cannot be empty string")
// 	}

// 	tracer := concurrency.NewTracer(task, fmt.Sprintf("(%s)", hostID), true).GoingIn()
// 	defer tracer.OnExitTrace()()
// 	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	// Removes master from cluster metadata
// 	var master *propertiesv2.Node
// 	err = c.UpdateMetadata(task, func() error {
// 		return c.Properties.Alter(Property.NodesV2, func(v interface{}) error {
// 			nodesV2 := v.(*propertiesv2.Nodes)
// 			found, idx := contains(nodesV2.Masters, hostID)
// 			if !found {
// 				return resources.ResourceNotFoundError("host", hostID)
// 			}
// 			master = nodesV2.Masters[idx]
// 			if idx < len(nodesV2.Masters)-1 {
// 				nodesV2.Masters = append(nodesV2.Masters[:idx], nodesV2.Masters[idx+1:]...)
// 			} else {
// 				nodesV2.Masters = nodesV2.Masters[:idx]
// 			}
// 			return nil
// 		})
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	// Starting from here, restore master in cluster metadata if exiting with error
// 	defer func() {
// 		if err != nil {
// 			derr := c.UpdateMetadata(task, func() error {
// 				return c.Properties.Alter(Property.NodesV2, func(v interface{}) error {
// 					nodesV2 := v.(*propertiesv2.Nodes)
// 					nodesV2.Masters = append(nodesV2.Masters, master)
// 					return nil
// 				})
// 			})
// 			if derr != nil {
// 				logrus.Errorf("failed to restore node ownership in cluster")
// 			}
// 			err = scerr.AddConsequence(err, derr)
// 		}
// 	}()

// 	// Finally delete host
// 	err = client.New().Host.Delete([]string{master.ID}, temporal.GetLongOperationTimeout())
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// VPL: moved in resources.Cluster
// // DeleteLastNode deletes the last Agent node added
// func (c *Controller) DeleteLastNode(task concurrency.Task, selectedMasterID string) (err error) {
// 	if c == nil {
// 		return scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	tracer := concurrency.NewTracer(task, fmt.Sprintf("('%s')", selectedMasterID), true).GoingIn()
// 	defer tracer.OnExitTrace()()
// 	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

// 	var node *propertiesv2.Node

// 	// Removed reference of the node from cluster metadata
// 	c.RLock(task)

// 	err = c.Properties.Inspect(Property.NodesV2, func(v interface{}) error {
// 		nodesV2 := v.(*propertiesv2.Nodes)
// 		node = nodesV2.PrivateNodes[len(nodesV2.PrivateNodes)-1]
// 		return nil
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	if selectedMasterID == "" {
// 		master, err := c.FindAvailableMaster(task)
// 		if err != nil {
// 			errDelNode := c.deleteNode(task, node, "")
// 			err = scerr.AddConsequence(err, errDelNode)
// 			return err
// 		}
// 		selectedMasterID = master.ID
// 	}

// 	return c.deleteNode(task, node, selectedMasterID)
// }

// VPL: moved in resources.Cluster
// // DeleteSpecificNode deletes the node specified by its ID
// func (c *Controller) DeleteSpecificNode(task concurrency.Task, hostID string, selectedMasterID string) (err error) {
// 	if c == nil {
// 		return scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if hostID == "" {
// 		return scerr.InvalidParameterError("hostID", "cannot be empty string")
// 	}
// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	tracer := concurrency.NewTracer(task, fmt.Sprintf("(%s)", hostID), true).GoingIn()
// 	defer tracer.OnExitTrace()()
// 	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

// 	var (
// 		node *propertiesv2.Node
// 	)

// 	c.RLock(task)
// 	err = c.Properties.Inspect(Property.NodesV2, func(v interface{}) error {
// 		nodesV2 := v.(*propertiesv2.Nodes)
// 		var (
// 			idx   int
// 			found bool
// 		)
// 		if found, idx = contains(nodesV2.PrivateNodes, hostID); !found {
// 			return scerr.NotFoundError(fmt.Sprintf("failed to find node '%s'", hostID))
// 		}
// 		node = nodesV2.PrivateNodes[idx]
// 		return nil
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	if selectedMasterID == "" {
// 		master, err := c.FindAvailableMaster(task)
// 		if err != nil {
// 			errDelNode := c.deleteNode(task, node, "")
// 			err = scerr.AddConsequence(err, errDelNode)
// 			return err
// 		}
// 		selectedMasterID = master.ID
// 	}

// 	return c.deleteNode(task, node, selectedMasterID)
// }

// DeleteNode deletes the node specified by its ID
func (c *controller) DeleteNode(task concurrency.Task, node *propertiesv2.Node, selectedMaster string) (err error) {
	if c == nil {
		return scerr.InvalidInstanceError()
	}
	defer scerr.OnPanic(&err)()

	if node == nil {
		return scerr.InvalidParameterError("node", "cannot be nil")
	}
	if task == nil {
		task = concurrency.RootTask()
	}

	tracer := concurrency.NewTracer(task, fmt.Sprintf("(%s, '%s')", node.Name, selectedMaster), true).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	// VPL: moved in resources.Cluster
	// // Removes node from cluster metadata (done before really deleting node to prevent operations on the node in parallel)
	// err = c.UpdateMetadata(task, func() error {
	// 	return c.Properties.Alter(Property.NodesV2, func(v interface{}) error {
	// 		nodesV2 := v.(*propertiesv2.Nodes)
	// 		length := len(nodesV2.PrivateNodes)
	// 		_, idx := contains(nodesV2.PrivateNodes, node.ID)
	// 		if idx < length-1 {
	// 			nodesV2.PrivateNodes = append(nodesV2.PrivateNodes[:idx], nodesV2.PrivateNodes[idx+1:]...)
	// 		} else {
	// 			nodesV2.PrivateNodes = nodesV2.PrivateNodes[:idx]
	// 		}
	// 		return nil
	// 	})
	// })
	// if err != nil {
	// 	return err
	// }

	// // Starting from here, restore node in cluster metadata if exiting with error
	// defer func() {
	// 	if err != nil {
	// 		derr := c.UpdateMetadata(task, func() error {
	// 			return c.Properties.Alter(Property.NodesV2, func(v interface{}) error {
	// 				nodesV2 := v.(*propertiesv2.Nodes)
	// 				nodesV2.PrivateNodes = append(nodesV2.PrivateNodes, node)
	// 				return nil
	// 			})
	// 		})
	// 		if derr != nil {
	// 			logrus.Errorf("failed to restore node ownership in cluster")
	// 		}
	// 		err = scerr.AddConsequence(err, derr)
	// 	}
	// }()

	// Leave node from cluster (ie leave Docker swarm), if selectedMaster isn't empty
	if selectedMaster != "" {
		err = c.foreman.leaveNodesFromList(task, []string{node.ID}, selectedMaster)
		if err != nil {
			return err
		}

		// Unconfigure node
		err = c.foreman.unconfigureNode(task, node.ID, selectedMaster)
		if err != nil {
			return err
		}
	}

	// Finally delete host
	err = client.New().Host.Delete([]string{node.ID}, temporal.GetLongOperationTimeout())
	if err != nil {
		if _, ok := err.(*scerr.ErrNotFound); ok {
			// host seems already deleted, so it's a success (handles the case where )
			return nil
		}
		return err
	}

	return nil
}

// VPL: moved to resources.Cluster
// // FIXME ROBUSTNESS All functions MUST propagate context
// // Delete destroys everything related to the infrastructure built for the Cluster
// func (c *Controller) Delete(task concurrency.Task) (err error) {
// 	if c == nil {
// 		return scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	tracer := concurrency.NewTracer(task, "", true).GoingIn()
// 	defer tracer.OnExitTrace()()
// 	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

// 	// Updates metadata
// 	err = c.UpdateMetadata(task, func() error {
// 		return c.Properties.Alter(Property.StateV1, func(v interface{}) error {
// 			v.(*propsv1.State).State = clusterstate.Removed
// 			return nil
// 		})
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	deleteNodeFunc := func(t concurrency.Task, params concurrency.TaskParameters) (concurrency.TaskResult, error) {
// 		hostId, ok := params.(string)
// 		if !ok {
// 			return nil, scerr.InvalidParameterError("params", "is not a string")
// 		}
// 		funcErr := c.DeleteSpecificNode(t, hostId, "")
// 		return nil, funcErr
// 	}
// 	deleteMasterFunc := func(t concurrency.Task, params concurrency.TaskParameters) (concurrency.TaskResult, error) {
// 		hostId, ok := params.(string)
// 		if !ok {
// 			return nil, scerr.InvalidParameterError("params", "is not a string")
// 		}
// 		funcErr := c.deleteMaster(t, hostId)
// 		return nil, funcErr
// 	}

// 	var cleaningErrors []error

// 	// Deletes the nodes
// 	list, err := c.ListNodeIDs(task)
// 	if err != nil {
// 		return err
// 	}
// 	if len(list) > 0 {
// 		var subtasks []concurrency.Task
// 		for _, v := range list {
// 			subtask, err := task.StartInSubTask(deleteNodeFunc, v)
// 			if err != nil {
// 				return err
// 			}
// 			subtasks = append(subtasks, subtask)
// 		}
// 		for _, s := range subtasks {
// 			_, subErr := s.Wait()
// 			if subErr != nil {
// 				cleaningErrors = append(cleaningErrors, subErr)
// 			}
// 		}
// 	}

// 	// Delete the Masters
// 	list, err = c.ListMasterIDs(task)
// 	if err != nil {
// 		return err
// 	}
// 	if len(list) > 0 {
// 		var subtasks []concurrency.Task
// 		for _, v := range list {
// 			subtask, err := task.StartInSubTask(deleteMasterFunc, v)
// 			if err != nil {
// 				return err
// 			}
// 			subtasks = append(subtasks, subtask)
// 		}
// 		for _, s := range subtasks {
// 			_, subErr := s.Wait()
// 			if subErr != nil {
// 				cleaningErrors = append(cleaningErrors, subErr)
// 			}
// 		}
// 	}

// 	// get access to metadata
// 	networkID := ""
// 	if c.Properties.Lookup(Property.NetworkV2) {
// 		err = c.Properties.Inspect(Property.NetworkV2, func(v interface{}) error {
// 			networkID = v.(*propertiesv2.Network).NetworkID
// 			return nil
// 		})
// 	} else {
// 		err = c.Properties.Inspect(Property.NetworkV1, func(v interface{}) error {
// 			networkID = v.(*propsv1.Network).NetworkID
// 			return nil
// 		})
// 	}
// 	if err != nil {
// 		cleaningErrors = append(cleaningErrors, err)
// 		return scerr.ErrListError(cleaningErrors)
// 	}

// 	// Deletes the network
// 	clientNetwork := client.New().Network
// 	retryErr := retry.WhileUnsuccessfulDelay5SecondsTimeout(
// 		func() error {
// 			return clientNetwork.Delete([]string{networkID}, temporal.GetExecutionTimeout())
// 		},
// 		temporal.GetHostTimeout(),
// 	)
// 	if retryErr != nil {
// 		cleaningErrors = append(cleaningErrors, retryErr)
// 		return scerr.ErrListError(cleaningErrors)
// 	}

// 	// Deletes the metadata
// 	err = c.DeleteMetadata(task)
// 	if err != nil {
// 		cleaningErrors = append(cleaningErrors, err)
// 		return scerr.ErrListError(cleaningErrors)
// 	}

// 	err = c.Lock(task)
// 	if err != nil {
// 		return err
// 	}
// 	defer func() {
// 		unlockErr := c.Unlock(task)
// 		if unlockErr != nil {
// 			logrus.Warn(unlockErr)
// 		}
// 		if err == nil && unlockErr != nil {
// 			err = unlockErr
// 		}
// 	}()

// 	c.service = nil

// 	return scerr.ErrListError(cleaningErrors)
// }

// VPL: moved to resources.Cluster
// // Stop stops the Cluster is its current state is compatible
// func (c *Controller) Stop(task concurrency.Task) (err error) {
// 	if c == nil {
// 		return scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	tracer := concurrency.NewTracer(task, "", true).GoingIn()
// 	defer tracer.OnExitTrace()()
// 	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

// 	// state, _ := c.ForceGetState(task)
// 	// if state == clusterstate.Stopped {
// 	// 	return nil
// 	// }

// 	// if state != clusterstate.Nominal && state != clusterstate.Degraded {
// 	// 	return fmt.Errorf("failed to stop Cluster because of it's current state: %s", state.String())
// 	// }

// 	// // Updates metadata to mark the cluster as Stopping
// 	// err = c.UpdateMetadata(task, func() error {
// 	// 	return c.Properties.Alter(Property.StateV1, func(v interface{}) error {
// 	// 		v.(*propsv1.State).State = clusterstate.Stopping
// 	// 		return nil
// 	// 	})
// 	// })
// 	// if err != nil {
// 	// 	return err
// 	// }

// 	// Stops the resources of the cluster

// 	var (
// 		nodes                         []*propertiesv2.Node
// 		masters                       []*propertiesv2.Node
// 		gatewayID, secondaryGatewayID string
// 	)
// 	err = c.Properties.Inspect(Property.NodesV2, func(v interface{}) error {
// 		nodesV2 := v.(*propertiesv2.Nodes)
// 		masters = nodesV2.Masters
// 		nodes = nodesV2.PrivateNodes
// 		return nil
// 	})
// 	if err != nil {
// 		return fmt.Errorf("failed to get list of hosts: %v", err)
// 	}
// 	if c.Properties.Lookup(Property.NetworkV2) {
// 		err = c.Properties.Inspect(Property.NetworkV2, func(v interface{}) error {
// 			networkV2 := v.(*propertiesv2.Network)
// 			gatewayID = networkV2.GatewayID
// 			secondaryGatewayID = networkV2.SecondaryGatewayID
// 			return nil
// 		})
// 	} else {
// 		err = c.Properties.Inspect(Property.NetworkV1, func(v interface{}) error {
// 			gatewayID = v.(*propsv1.Network).GatewayID
// 			return nil
// 		})
// 	}
// 	if err != nil {
// 		return err
// 	}

// 	// Stop nodes
// 	taskGroup, err := concurrency.NewTaskGroup(task)
// 	if err != nil {
// 		return err
// 	}

// 	// FIXME introduce status

// 	for _, n := range nodes {
// 		_, err = taskGroup.Start(c.asyncStopHost, n.ID)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	// Stop masters
// 	for _, n := range masters {
// 		_, err = taskGroup.Start(c.asyncStopHost, n.ID)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	// Stop gateway(s)
// 	_, err = taskGroup.Start(c.asyncStopHost, gatewayID)
// 	if err != nil {
// 		return err
// 	}
// 	if secondaryGatewayID != "" {
// 		_, err = taskGroup.Start(c.asyncStopHost, secondaryGatewayID)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	_, err = taskGroup.Wait()
// 	if err != nil {
// 		return err
// 	}

// 	// Updates metadata to mark the cluster as Stopped
// 	return c.UpdateMetadata(task, func() error {
// 		return c.Properties.Alter(Property.StateV1, func(v interface{}) error {
// 			v.(*propsv1.State).State = clusterstate.Stopped
// 			state = clusterstate.Stopped
// 			return nil
// 		})
// 	})
// }

//VPL: moved in resources.Cluster
// func (c *Controller) asyncStopHost(task concurrency.Task, params concurrency.TaskParameters) (concurrency.TaskResult, error) {
// 	return nil, c.service.StopHost(params.(string))
// }

//VPL: moved in resources.Cluster
// // Start starts the Cluster
// func (c *Controller) Start(task concurrency.Task) (err error) {
// 	if c == nil {
// 		return scerr.InvalidInstanceError()
// 	}
// 	defer scerr.OnPanic(&err)()

// 	if task == nil {
// 		task = concurrency.RootTask()
// 	}

// 	tracer := concurrency.NewTracer(task, "", true).GoingIn()
// 	defer tracer.OnExitTrace()()
// 	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

// 	state, err := c.ForceGetState(task)
// 	if err != nil {
// 		return err
// 	}
// 	if state == clusterstate.Nominal || state == clusterstate.Degraded || state == clusterstate.Starting {
// 		return nil
// 	}
// 	if state != clusterstate.Stopped {
// 		return fmt.Errorf("failed to start Cluster because of it's current state: %s", state.String())
// 	}

// 	// Updates metadata to mark the cluster as Starting
// 	err = c.UpdateMetadata(task, func() error {
// 		return c.Properties.Alter(Property.StateV1, func(v interface{}) error {
// 			v.(*propsv1.State).State = clusterstate.Starting
// 			return nil
// 		})
// 	})
// 	if err != nil {
// 		return err
// 	}

// 	// Starts the resources of the cluster

// 	var (
// 		nodes                         []*propertiesv2.Node
// 		masters                       []*propertiesv2.Node
// 		gatewayID, secondaryGatewayID string
// 	)
// 	err = c.Properties.Inspect(Property.NodesV2, func(v interface{}) error {
// 		nodesV2 := v.(*propertiesv2.Nodes)
// 		masters = nodesV2.Masters
// 		nodes = nodesV2.PrivateNodes
// 		return nil
// 	})
// 	if err != nil {
// 		return fmt.Errorf("failed to get list of hosts: %v", err)
// 	}
// 	if c.Properties.Lookup(Property.NetworkV2) {
// 		err = c.Properties.Inspect(Property.NetworkV2, func(v interface{}) error {
// 			networkV2 := v.(*propertiesv2.Network)
// 			gatewayID = networkV2.GatewayID
// 			secondaryGatewayID = networkV2.SecondaryGatewayID
// 			return nil
// 		})
// 	} else {
// 		err = c.Properties.Inspect(Property.NetworkV1, func(v interface{}) error {
// 			gatewayID = v.(*propsv1.Network).GatewayID
// 			return nil
// 		})
// 	}
// 	if err != nil {
// 		return err
// 	}

// 	// FIXME introduce status

// 	// Start gateway(s)
// 	taskGroup, err := concurrency.NewTaskGroup(task)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = taskGroup.Start(c.asyncStartHost, gatewayID)
// 	if err != nil {
// 		return err
// 	}
// 	if secondaryGatewayID != "" {
// 		_, err = taskGroup.Start(c.asyncStartHost, secondaryGatewayID)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	// Start masters
// 	for _, n := range masters {
// 		_, err = taskGroup.Start(c.asyncStopHost, n.ID)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	// Start nodes
// 	for _, n := range nodes {
// 		_, err = taskGroup.Start(c.asyncStopHost, n.ID)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	_, err = taskGroup.Wait()
// 	if err != nil {
// 		return err
// 	}

// 	// Updates metadata to mark the cluster as Stopped
// 	return c.UpdateMetadata(task, func() error {
// 		return c.Properties.Alter(Property.StateV1, func(v interface{}) error {
// 			v.(*propsv1.State).State = clusterstate.Nominal
// 			return nil
// 		})
// 	})
// }

// VPL: moved to resources.Cluster
// func (c *Controller) asyncStartHost(task concurrency.Task, params concurrency.TaskParameters) (concurrency.TaskResult, error) {
// 	return nil, c.service.StartHost(params.(string))
// }

// // sanitize tries to rebuild manager struct based on what is available on ObjectStorage
// func (c *Controller) Sanitize(data *Metadata) error {

// 	core := data.Get()
// 	instance := &Cluster{
// 		Core:     core,
// 		metadata: data,
// 	}
// 	instance.reset()

// 	if instance.manager == nil {
// 		var mgw *providermetadata.Gateway
// 		mgw, err := providermetadata.LoadGateway(svc, instance.Core.NetworkID)
// 		if err != nil {
// 			return err
// 		}
// 		gw := mgw.Get()
// 		hm := providermetadata.NewHost(svc)
// 		hosts := []*resources.Host{}
// 		err = hm.Browse(func(h *resources.Host) error {
// 			if strings.HasPrefix(h.Name, instance.Core.Name+"-") {
// 				hosts = append(hosts, h)
// 			}
// 			return nil
// 		})
// 		if err != nil {
// 			return err
// 		}
// 		if len(hosts) == 0 {
// 			return fmt.Errorf("failed to find hosts belonging to cluster")
// 		}

// 		// We have hosts, fill the manager
// 		masterIDs := []string{}
// 		masterIPs := []string{}
// 		privateNodeIPs := []string{}
// 		publicNodeIPs := []string{}
// 		defaultNetworkIP := ""
// 		err = gw.Properties.Inspect(HostProperty.NetworkV1, func(v interface{}) error {
// 			hostNetworkV1 := v.(*propsv1.HostNetwork)
// 			defaultNetworkIP = hostNetworkV1.IPv4Addresses[hostNetworkV1.DefaultNetworkID]
// 			for _, h := range hosts {
// 				if strings.HasPrefix(h.Name, instance.Core.Name+"-master-") {
// 					masterIDs = append(masterIDs, h.ID)
// 					masterIPs = append(masterIPs, defaultNetworkIP)
// 				} else if strings.HasPrefix(h.Name, instance.Core.Name+"-node-") {
// 					privateNodeIPs = append(privateNodeIPs, defaultNetworkIP)
// 				} else if strings.HasPrefix(h.Name, instance.Core.Name+"-pubnode-") {
// 					publicNodeIPs = append(privateNodeIPs, defaultNetworkIP)
// 				}
// 			}
// 			return nil
// 		})
// 		if err != nil {
// 			return fmt.Errorf("failed to update metadata of cluster '%s': %s", instance.Core.Name, err.Error())
// 		}

// 		newManager := &managerData{
// 			BootstrapID:      gw.ID,
// 			BootstrapIP:      defaultNetworkIP,
// 			MasterIDs:        masterIDs,
// 			MasterIPs:        masterIPs,
// 			PrivateNodeIPs:   privateNodeIPs,
// 			PublicNodeIPs:    publicNodeIPs,
// 			MasterLastIndex:  len(masterIDs),
// 			PrivateLastIndex: len(privateNodeIPs),
// 			PublicLastIndex:  len(publicNodeIPs),
// 		}
// 		logrus.Debugf("updating metadata...")
// 		err = instance.updateMetadata(func() error {
// 			instance.manager = newManager
// 			return nil
// 		})
// 		if err != nil {
// 			return fmt.Errorf("failed to update metadata of cluster '%s': %s", instance.Core.Name, err.Error())
// 		}
// 	}
// 	return nil
// }

// ExecuteScript executes the script template with the parameters on tarGetHost
func (c *controller) ExecuteScript(
	ctx context.Context, box *rice.Box, funcMap map[string]interface{}, tmplName string, vars data.Map,
	hostID string,
) (errCode int, stdOut string, stdErr string, err error) {

	tracer := concurrency.NewTracer(nil, "("+hostID+")", true).GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	if b == nil {
		return 0, "", "", scerr.InvalidInstanceError()
	}

	// Configures reserved_BashLibrary template var
	bashLibrary, err := system.GetBashLibrary()
	if err != nil {
		return 0, "", "", err
	}
	vars["reserved_BashLibrary"] = bashLibrary

	script, path, err := realizeTemplate(box, funcMap, tmplName, data, tmplName)
	if err != nil {
		return 0, "", "", err
	}

	hidesOutput := strings.Contains(script, "set +x\n")
	if hidesOutput {
		script = strings.Replace(script, "set +x\n", "\n", 1)
		if strings.Contains(script, "exec 2>&1\n") {
			script = strings.Replace(script, "exec 2>&1\n", "exec 2>&7\n", 1)
		}
	}

	err = uploadScriptToFileInHost(script, hostID, path)
	if err != nil {
		return 0, "", "", err
	}

	cmd := fmt.Sprintf("sudo chmod u+rx %s;sudo bash %s;exit ${PIPESTATUS}", path, path)
	if hidesOutput {
		cmd = fmt.Sprintf("sudo chmod u+rx %s;sudo bash -c \"BASH_XTRACEFD=7 %s 7> /tmp/captured 2>&7\";echo ${PIPESTATUS} > /tmp/errc;cat /tmp/captured; sudo rm /tmp/captured;exit `cat /tmp/errc`", path, path)
	}

	return client.New().SSH.Run(ctx, hostID, cmd, temporal.GetConnectionTimeout(), 2*temporal.GetLongOperationTimeout())
}
