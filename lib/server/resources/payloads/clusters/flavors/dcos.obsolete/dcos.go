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

package dcos

import (
	"bytes"
	"fmt"
	"strings"
	"sync/atomic"
	txttmpl "text/template"

	"github.com/CS-SI/SafeScale/lib/utils/scerr"

	"github.com/CS-SI/SafeScale/lib/client"
	"github.com/CS-SI/SafeScale/lib/server/resources/abstracts"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clustercomplexity"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clusternodetype"
	"github.com/CS-SI/SafeScale/lib/server/resources/payloads/clusters/flavors"
	"github.com/CS-SI/SafeScale/lib/server/resources/payloads/clusters/flavors/dcos/enums/ErrorCode"
	"github.com/CS-SI/SafeScale/lib/utils/concurrency"
	"github.com/CS-SI/SafeScale/lib/utils/data"
	"github.com/CS-SI/SafeScale/lib/utils/template"
	"github.com/CS-SI/SafeScale/lib/utils/temporal"
	rice "github.com/GeertJohan/go.rice"
)

//go:generate rice embed-go

const (
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
	Makers = flavors.Makers{
		MinimumRequiredServers:      minimumRequiredServers,
		DefaultGatewaySizing:        gatewaySizing,
		DefaultMasterSizing:         masterSizing,
		DefaultNodeSizing:           nodeSizing,
		DefaultImage:                defaultImage,
		ConfigureGateway:            configureGateway,
		ConfigureMaster:             configureMaster,
		ConfigureNode:               configureNode,
		GetTemplateBox:              getTemplateBox,
		GetGlobalSystemRequirements: getGlobalSystemRequirements,
		GetNodeInstallationScript:   getNodeInstallationScript,
		GetState:                    getState,
	}
)

func minimumRequiredServers(task concurrency.Task, f flavors.Foreman) (uint, uint, uint, error) {
	var masterCount uint
	var privateNodeCount uint

	complexity, err := f.Cluster().Complexity(task)
	if err != nil {
		return 0, 0, 0, err
	}
	switch complexity {
	case clustercomplexity.Small:
		masterCount = 1
		privateNodeCount = 2
	case clustercomplexity.Normal:
		masterCount = 3
		privateNodeCount = 4
	case clustercomplexity.Large:
		masterCount = 5
		privateNodeCount = 6
	}
	return masterCount, privateNodeCount, 0, nil
}

func gatewaySizing(task concurrency.Task, f flavors.Foreman) abstracts.SizingRequirements {
	return abstracts.SizingRequirements{
		MinCores:    2,
		MaxCores:    4,
		MinRAMSize:  7.0,
		MaxRAMSize:  16.0,
		MinDiskSize: 50,
		MinGPU:      -1,
	}
}

func masterSizing(task concurrency.Task, f flavors.Foreman) abstracts.SizingRequirements {
	return abstracts.SizingRequirements{
		MinCores:    4,
		MaxCores:    8,
		MinRAMSize:  15.0,
		MaxRAMSize:  32.0,
		MinDiskSize: 800,
		MinGPU:      -1,
	}
}

func nodeSizing(task concurrency.Task, f flavors.Foreman) abstracts.SizingRequirements {
	return abstracts.SizingRequirements{
		MinCores:    2,
		MaxCores:    4,
		MinRAMSize:  15.0,
		MaxRAMSize:  32.0,
		MinDiskSize: 80,
		MinGPU:      -1,
	}
}

func defaultImage(task concurrency.Task, f flavors.Foreman) string {
	return centos
}

func configureMaster(task concurrency.Task, f flavors.Foreman, index uint, host *abstracts.Host) error {
	box, err := getTemplateBox()
	if err != nil {
		return err
	}

	ctx, err := task.Context()
	if err != nil {
		return err
	}

	hostLabel := fmt.Sprintf("master #%d (%s)", index, host.Name)

	netCfg, err := f.Cluster().NetworkConfig(task)
	if err != nil {
		return err
	}
	retcode, stdout, stderr, err := f.ExecuteScript(ctx, box, funcMap, "dcos_configure_master.sh", data.Map{
		"BootstrapIP":   netCfg.GatewayIP,
		"BootstrapPort": bootstrapHTTPPort,
	}, host.ID)
	if err != nil {
		return fmt.Errorf("[%s] failed to remotely run configuration script: %s", hostLabel, err.Error())
	}
	if retcode != 0 {
		return handleExecuteScriptReturn(retcode, stdout, stderr, err, fmt.Sprintf("[%s] scripted Master configuration", hostLabel))
	}
	return nil
}

func configureNode(task concurrency.Task, f flavors.Foreman, index uint, host *abstracts.Host) error {
	hostLabel := fmt.Sprintf("node #%d (%s)", index, host.Name)

	ctx, err := task.Context()
	if err != nil {
		return err
	}

	box, err := getTemplateBox()
	if err != nil {
		return err
	}
	netCfg, err := f.Cluster().NetworkConfig(task)
	if err != nil {
		return err
	}
	retcode, stdout, stderr, err := f.ExecuteScript(ctx, box, funcMap, "dcos_configure_node.sh", data.Map{
		"BootstrapIP":   netCfg.GatewayIP,
		"BootstrapPort": bootstrapHTTPPort,
	}, host.ID)
	if err != nil {
		return fmt.Errorf("[%s] failed to remotely run configuration script: %s", hostLabel, err.Error())
	}
	if retcode != 0 {
		return handleExecuteScriptReturn(retcode, stdout, stderr, err, fmt.Sprintf("[%s] scripted Agent configuration", hostLabel))
	}

	return nil
}

func handleExecuteScriptReturn(retcode int, stdout string, stderr string, err error, msg string) error {
	if retcode == 0 {
		return nil
	}

	richErrc := fmt.Sprintf("%d", retcode)
	if retcode < int(ErrorCode.NextErrorCode) {
		errCode := ErrorCode.Enum(retcode)
		richErrc = fmt.Sprintf("%d (%s)", errCode, errCode.String())
	}

	collected := []string{}
	if stdout != "" {
		errLines := strings.Split(stdout, "\n")
		for _, errline := range errLines {
			if strings.Contains(errline, "An error occurred") {
				collected = append(collected, errline)
			}
		}
	}
	if stderr != "" {
		errLines := strings.Split(stderr, "\n")
		for _, errline := range errLines {
			if strings.Contains(errline, "An error occurred") {
				collected = append(collected, errline)
			}
		}
	}

	if len(collected) > 0 {
		if err != nil {
			return scerr.Wrap(err, fmt.Sprintf("%s: failed with error code %s, std errors [%s]", msg, richErrc, strings.Join(collected, ";")))
		}
		return fmt.Errorf("%s: failed with error code %s, std errors [%s]", msg, richErrc, strings.Join(collected, ";"))
	}

	if err != nil {
		return scerr.Wrap(err, fmt.Sprintf("%s: failed with error code %s", msg, richErrc))
	}
	return fmt.Errorf("%s: failed with error code %s", msg, richErrc)
}

func getNodeInstallationScript(task concurrency.Task, f flavors.Foreman, hostType clusternodetype.Enum) (string, data.Map) {
	nodeData := data.Map{}

	var script string
	switch hostType {
	case clusternodetype.Master:
		script = "dcos_install_master.sh"
	case clusternodetype.Node:
		script = "dcos_install_node.sh"
	}
	return script, nodeData
}

func configureGateway(task concurrency.Task, f flavors.Foreman) error {
	globalSystemRequirements, err := getGlobalSystemRequirements(task, f)
	if err != nil {
		return err
	}
	box, err := getTemplateBox()
	if err != nil {
		return err
	}

	ctx, err := task.Context()
	if err != nil {
		return err
	}

	var dnsServers []string
	cluster := f.Cluster()
	cfg, err := cluster.Service().GetConfigurationOptions()
	if err == nil {
		dnsServers = cfg.GetSliceOfStrings("DNSList")
	}

	netCfg, err := cluster.NetworkConfig(task)
	if err != nil {
		return err
	}

	identity, err := cluster.Identity(task)
	if err != nil {
		return err
	}

	list, err := cluster.ListMasterIPs(task)
	if err != nil {
		return err
	}
	gwData := data.Map{
		"reserved_CommonRequirements": globalSystemRequirements,
		// "BootstrapIP":                 netCfg.PrimaryGatewayPrivateIP,
		"BootstrapIP":   netCfg.GatewayIP,
		"BootstrapPort": bootstrapHTTPPort,
		"ClusterName":   identity.Name,
		"MasterIPs":     list.Values(),
		"DNSServerIPs":  dnsServers,
		// "DefaultRouteIP": netCfg.VIP.PrivateIP,
		"DefaultRouteIP": netCfg.GatewayIP,
		"SSHPrivateKey":  identity.Keypair.PrivateKey,
		"SSHPublicKey":   identity.Keypair.PublicKey,
	}
	retcode, stdout, stderr, err := f.ExecuteScript(ctx, box, funcMap, "dcos_prepare_bootstrap.sh", gwData, netCfg.GatewayID)
	if err != nil {
		return err
	}
	if retcode != 0 {
		return handleExecuteScriptReturn(retcode, stdout, stderr, err, fmt.Sprintf("[%s] scripted gateway configuration", "gateway"))
	}

	return nil
}

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
func getGlobalSystemRequirements(task concurrency.Task, f flavors.Foreman) (string, error) {
	anon := globalSystemRequirementsContent.Load()
	if anon == nil {
		// find the rice.Box
		box, err := getTemplateBox()
		if err != nil {
			return "", err
		}

		// We will need information about cluster network
		cluster := f.Cluster()
		netCfg, err := cluster.NetworkConfig(task)
		if err != nil {
			return "", err
		}

		// get file contents as string
		tmplString, err := box.String("dcos_install_requirements.sh")
		if err != nil {
			return "", fmt.Errorf("error loading script template: %s", err.Error())
		}

		// parse then execute the template
		tmplPrepared, err := txttmpl.New("install_requirements").Funcs(template.MergeFuncs(funcMap, false)).Parse(tmplString)
		if err != nil {
			return "", fmt.Errorf("error parsing script template: %s", err.Error())
		}
		dataBuffer := bytes.NewBufferString("")
		identity, err := cluster.Identity(task)
		if err != nil {
			return "", err
		}

		err = tmplPrepared.Execute(dataBuffer, map[string]interface{}{
			"CIDR":          netCfg.CIDR,
			"Username":      "cladm",
			"CladmPassword": identity.AdminPassword,
			"SSHPublicKey":  identity.Keypair.PublicKey,
			"SSHPrivateKey": identity.Keypair.PrivateKey,
		})
		if err != nil {
			return "", fmt.Errorf("error realizing script template: %s", err.Error())
		}
		globalSystemRequirementsContent.Store(dataBuffer.String())
		anon = globalSystemRequirementsContent.Load()
	}
	return anon.(string), nil
}

// getState returns the current state of the cluster
// This method will trigger a effective state collection at each call
func getState(task concurrency.Task, f flavors.Foreman) (clustestate.Enum, error) {
	var (
		retcode int
		ran     bool // Tells if command has been run on remote host
		stderr  string
	)

	cmd := "/opt/mesosphere/bin/dcos-diagnostics --diag"
	safescaleClt := client.New()
	safescaleCltHost := safescaleClt.Host
	master, err := f.Cluster().FindAvailableMaster(task)
	if err != nil {
		return clustestate.Unknown, err
	}
	sshCfg, err := safescaleCltHost.SSHConfig(master.ID)
	if err != nil {
		return clustestate.Error, scerr.Wrap(err, fmt.Sprintf("failed to get ssh config to connect to master '%s': %s", master.ID, err.Error()))

	}

	ctx, err := task.Context()
	if err != nil {
		return clustestate.Error, scerr.Wrap(err, fmt.Sprintf("failed to get valid context : %s", err.Error()))
	}

	_, err = sshCfg.WaitServerReady(ctx, "ready", temporal.GetContextTimeout())
	if err != nil {
		return clustestate.Error, err
	}
	retcode, _, stderr, err = safescaleClt.SSH.Run(ctx, master.ID, cmd, temporal.GetConnectionTimeout(), temporal.GetExecutionTimeout())
	if err != nil {
		return clustestate.Error, scerr.Wrap(err, fmt.Sprintf("failed to run remote command to get cluster state: %v\n%s", err, stderr))
	}
	ran = true

	if ran && retcode == 0 {
		return clustestate.Nominal, nil
	}
	return clustestate.Error, err
}
