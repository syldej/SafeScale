/*
 * Copyright 2018-2019, CS Systemes d'Information, http://www.c-s.fr
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

package dcos

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"time"

	txttmpl "text/template"

	rice "github.com/GeertJohan/go.rice"
	log "github.com/sirupsen/logrus"

	"github.com/CS-SI/SafeScale/iaas/resources"
	pb "github.com/CS-SI/SafeScale/safescale"
	"github.com/CS-SI/SafeScale/safescale/client"
	"github.com/CS-SI/SafeScale/safescale/server/cluster/control"
	"github.com/CS-SI/SafeScale/safescale/server/cluster/enums/ClusterState"
	"github.com/CS-SI/SafeScale/safescale/server/cluster/enums/Complexity"
	"github.com/CS-SI/SafeScale/safescale/server/cluster/enums/NodeType"
	"github.com/CS-SI/SafeScale/safescale/server/cluster/flavors/dcos/enums/ErrorCode"
	"github.com/CS-SI/SafeScale/utils/concurrency"
	"github.com/CS-SI/SafeScale/utils/template"
)

//go:generate rice embed-go

const (
	dcosVersion string = "1.11.6"

	bootstrapHTTPPort = 10080

	centos = "CentOS 7.3"
)

var (
	// templateBox is the rice box to use in this package
	templateBox atomic.Value

	// globalSystemRequirementsContent contains the script to install/configure Core features
	globalSystemRequirementsContent atomic.Value

	// funcMap defines the custom functions to be used in templates
	funcMap = txttmpl.FuncMap{
		"errcode": func(msg string) int {
			if code, ok := ErrorCode.StringMap[msg]; ok {
				return int(code)
			}
			return 1023
		},
	}

	// Makers initializes the control.Makers struct to construct a DCOS cluster
	Makers = control.Makers{
		MinimumRequiredServers:      minimumRequiredServers,
		DefaultGatewaySizing:        gatewaySizing,
		DefaultMasterSizing:         masterSizing,
		DefaultNodeSizing:           nodeSizing,
		DefaultImage:                defaultImage,
		ConfigureMaster:             configureMaster,
		ConfigureNode:               configureNode,
		GetTemplateBox:              getTemplateBox,
		GetGlobalSystemRequirements: getGlobalSystemRequirements,
		GetNodeInstallationScript:   getNodeInstallationScript,
	}
)

func minimumRequiredServers(task concurrency.Task, foreman control.Foreman) (int, int, int) {
	masterCount := 0
	privateNodeCount := 0

	switch foreman.Cluster().GetIdentity(task).Complexity {
	case Complexity.Small:
		masterCount = 1
		privateNodeCount = 2
	case Complexity.Normal:
		masterCount = 3
		privateNodeCount = 4
	case Complexity.Large:
		masterCount = 5
		privateNodeCount = 6
	}
	return masterCount, privateNodeCount, 0
}

func gatewaySizing(task concurrency.Task, foreman control.Foreman) resources.HostDefinition {
	return resources.HostDefinition{
		Cores:    2,
		RAMSize:  15.0,
		DiskSize: 60,
	}
}

func masterSizing(task concurrency.Task, foreman control.Foreman) resources.HostDefinition {
	return resources.HostDefinition{
		Cores:    4,
		RAMSize:  15.0,
		DiskSize: 100,
	}
}

func nodeSizing(task concurrency.Task, foreman control.Foreman) resources.HostDefinition {
	return resources.HostDefinition{
		Cores:    4,
		RAMSize:  15.0,
		DiskSize: 100,
	}
}

func defaultImage(task concurrency.Task, foreman control.Foreman) string {
	return centos
}

func configureMaster(task concurrency.Task, foreman control.Foreman, index int, host *pb.Host) error {
	box, err := getTemplateBox()
	if err != nil {
		return err
	}

	hostLabel := fmt.Sprintf("master #%d (%s)", index, host.Name)

	retcode, _, _, err := foreman.ExecuteScript(box, funcMap, "dcos_configure_master.sh", map[string]interface{}{
		"BootstrapIP":   foreman.Cluster().GetNetworkConfig(task).GatewayIP,
		"BootstrapPort": bootstrapHTTPPort,
	}, host.Id)
	if err != nil {
		log.Debugf("[%s] failed to remotely run configuration script: %s", hostLabel, err.Error())
		return err
	}
	if retcode != 0 {
		if retcode < int(ErrorCode.NextErrorCode) {
			errcode := ErrorCode.Enum(retcode)
			log.Debugf("[%s] configuration failed:\nretcode:%d (%s)", hostLabel, errcode, errcode.String())
			return fmt.Errorf("scripted Master configuration failed with error code %d (%s)", errcode, errcode.String())
		}

		log.Debugf("[%s] configuration failed:\nretcode=%d", hostLabel, retcode)
		return fmt.Errorf("scripted Master configuration failed with error code %d", retcode)
	}
	return nil
}

func configureNode(
	task concurrency.Task,
	foreman control.Foreman,
	index int, host *pb.Host, nodeType NodeType.Enum, nodeTypeStr string,
) error {

	var publicStr string
	if nodeType == NodeType.PublicNode {
		publicStr = "yes"
	} else {
		publicStr = "no"
	}

	hostLabel := fmt.Sprintf("%s node #%d (%s)", nodeTypeStr, index, host.Name)

	box, err := getTemplateBox()
	if err != nil {
		return err
	}
	retcode, _, _, err := foreman.ExecuteScript(box, funcMap, "dcos_configure_node.sh", map[string]interface{}{
		"PublicNode":    publicStr,
		"BootstrapIP":   foreman.Cluster().GetNetworkConfig(task).GatewayIP,
		"BootstrapPort": bootstrapHTTPPort,
	}, host.Id)
	if err != nil {
		log.Debugf("[%s] failed to remotely run configuration script: %s\n", hostLabel, err.Error())
		return err
	}
	if retcode != 0 {
		if retcode < int(ErrorCode.NextErrorCode) {
			errcode := ErrorCode.Enum(retcode)
			log.Debugf("[%s] configuration failed: retcode: %d (%s)", hostLabel, errcode, errcode.String())
			return fmt.Errorf("scripted Agent configuration failed with error code %d (%s)", errcode, errcode.String())
		}
		log.Debugf("[%s] configuration failed: retcode=%d", hostLabel, retcode)
		return fmt.Errorf("scripted Agent configuration failed with error code '%d'", retcode)
	}

	return nil
}

func getNodeInstallationScript(task concurrency.Task, foreman control.Foreman, hostType NodeType.Enum) (string, map[string]interface{}) {
	data := map[string]interface{}{
		"DCOSVersion": dcosVersion,
	}

	var script string
	switch hostType {
	case NodeType.Gateway:
		script = "dcos_prepare_bootstrap.sh"
	case NodeType.Master:
		script = "dcos_install_master.sh"
	case NodeType.PrivateNode:
		fallthrough
	case NodeType.PublicNode:
		script = "dcos_install_node.sh"
	}
	return script, data
}

func configureGateway(task concurrency.Task, foreman control.Foreman) error {
	globalSystemRequirements, err := getGlobalSystemRequirements(task, foreman)
	if err != nil {
		return err
	}
	box, err := getTemplateBox()
	if err != nil {
		return err
	}

	var dnsServers []string
	cluster := foreman.Cluster()
	cfg, err := cluster.GetService(task).GetCfgOpts()
	if err == nil {
		dnsServers = cfg.GetSliceOfStrings("DNSList")
	}
	netCfg := cluster.GetNetworkConfig(task)
	data := map[string]interface{}{
		"GlobalSystemRequirements": *globalSystemRequirements,
		"BootstrapIP":              netCfg.GatewayIP,
		"BootstrapPort":            bootstrapHTTPPort,
		"ClusterName":              cluster.GetIdentity(task).Name,
		"MasterIPs":                cluster.ListMasterIPs(task),
		"DNSServerIPs":             dnsServers,
	}
	retcode, _, _, err := foreman.ExecuteScript(box, funcMap, "dcos_prepare_bootstrap.sh", data, netCfg.GatewayID)
	if err != nil {
		log.Errorf("[gateway] configuration failed: %s", err.Error())
		return err
	}
	if retcode != 0 {
		if retcode < int(ErrorCode.NextErrorCode) {
			errcode := ErrorCode.Enum(retcode)
			log.Errorf("[gateway] configuration failed:\nretcode=%d (%s)", errcode, errcode.String())
			return fmt.Errorf("scripted gateway configuration failed with error code %d (%s)", errcode, errcode.String())
		}

		log.Errorf("[gateway] configuration failed:\nretcode=%d", retcode)
		return fmt.Errorf("scripted gateway configuration failed with error code %d", retcode)
	}

	return nil
}

// TODO: make templateBox an AtomicValue
func getTemplateBox() (*rice.Box, error) {
	anon := templateBox.Load()
	if anon == nil {
		// Note: path MUST be literal for rice to work
		b, err := rice.FindBox("../dcos/scripts")
		if err != nil {
			return nil, err
		}
		templateBox.Store(b)
		anon = templateBox.Load()
	}
	return anon.(*rice.Box), nil
}

// getGlobalSystemRequirements returns the string corresponding to the script dcos_install_requirements.sh
// which installs common features (docker in particular)
func getGlobalSystemRequirements(task concurrency.Task, foreman control.Foreman) (*string, error) {
	anon := globalSystemRequirementsContent.Load()
	if anon == nil {
		// find the rice.Box
		box, err := getTemplateBox()
		if err != nil {
			return nil, err
		}

		// get file contents as string
		tmplString, err := box.String("dcos_install_requirements.sh")
		if err != nil {
			return nil, fmt.Errorf("error loading script template: %s", err.Error())
		}

		// parse then execute the template
		tmplPrepared, err := txttmpl.New("install_requirements").Funcs(template.MergeFuncs(funcMap, false)).Parse(tmplString)
		if err != nil {
			return nil, fmt.Errorf("error parsing script template: %s", err.Error())
		}
		dataBuffer := bytes.NewBufferString("")
		cluster := foreman.Cluster()
		identity := cluster.GetIdentity(task)
		err = tmplPrepared.Execute(dataBuffer, map[string]interface{}{
			"CIDR":          cluster.GetNetworkConfig(task).CIDR,
			"Username":      "cladm",
			"CladmPassword": identity.AdminPassword,
			"SSHPublicKey":  identity.Keypair.PublicKey,
			"SSHPrivateKey": identity.Keypair.PrivateKey,
		})
		if err != nil {
			return nil, fmt.Errorf("error realizing script template: %s", err.Error())
		}
		result := dataBuffer.String()
		globalSystemRequirementsContent.Store(&result)
		anon = globalSystemRequirementsContent.Load()
	}
	return anon.(*string), nil
}

// getState returns the current state of the cluster
// This method will trigger a effective state collection at each call
func getState(task concurrency.Task, foreman control.Foreman) (ClusterState.Enum, error) {
	var (
		retcode int
		ran     bool // Tells if command has been run on remote host
		stderr  string
	)

	cmd := "/opt/mesosphere/bin/dcos-diagnostics --diag"
	safescaleClt := client.New()
	safescaleCltHost := safescaleClt.Host
	masterID, err := foreman.Cluster().FindAvailableMaster(task)
	if err != nil {
		return ClusterState.Unknown, err
	}
	sshCfg, err := safescaleCltHost.SSHConfig(masterID)
	if err != nil {
		log.Errorf("failed to get ssh config to connect to master '%s': %s", masterID, err.Error())
		return ClusterState.Error, err

	}
	err = sshCfg.WaitServerReady(2 * time.Minute)
	if err == nil {
		if err != nil {
			return ClusterState.Error, err
		}
		retcode, _, stderr, err = safescaleClt.Ssh.Run(masterID, cmd, client.DefaultConnectionTimeout, client.DefaultExecutionTimeout)
		if err != nil {
			log.Errorf("failed to run remote command to get cluster state: %v\n%s", err, stderr)
			return ClusterState.Error, err
		}
		ran = true
	}

	if ran && retcode == 0 {
		return ClusterState.Nominal, nil
	}
	return ClusterState.Error, err
}