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

package swarm

/*
 * Implements a swarmCluster of hosts without swarmCluster management environment
 */

import (
	"bytes"
	"fmt"
	"sync/atomic"
	txttmpl "text/template"

	rice "github.com/GeertJohan/go.rice"
	// log "github.com/sirupsen/logrus"

	"github.com/CS-SI/SafeScale/lib/server/resources/abstracts"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clustercomplexity"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clusternodetype"
	"github.com/CS-SI/SafeScale/lib/server/resources/payloads/clusters/flavors"
	"github.com/CS-SI/SafeScale/lib/utils/concurrency"
	"github.com/CS-SI/SafeScale/lib/utils/data"
)

//go:generate rice embed-go

const (
// tempFolder = "/opt/safescale/var/tmp/"
)

var (
	templateBox atomic.Value

	// GlobalSystemRequirementsContent *string
	globalSystemRequirementsContent atomic.Value

	// Makers initializes a flavors.Makers struct to construct a Docker Swarm Cluster
	Makers = flavors.Makers{
		MinimumRequiredServers:      minimumRequiredServers,
		DefaultGatewaySizing:        gatewaySizing,
		DefaultNodeSizing:           nodeSizing,
		DefaultImage:                defaultImage,
		GetTemplateBox:              getTemplateBox,
		GetGlobalSystemRequirements: getGlobalSystemRequirements,
		GetNodeInstallationScript:   getNodeInstallationScript,
	}
)

func minimumRequiredServers(task concurrency.Task, f flavors.Foreman) (uint, uint, uint, error) {
	complexity, err := f.Cluster().Complexity(task)
	if err != nil {
		return 0, 0, 0, err
	}

	var masterCount, privateNodeCount uint
	switch complexity {
	case clustercomplexity.Small:
		masterCount = 1
		privateNodeCount = 1
	case clustercomplexity.Normal:
		masterCount = 3
		privateNodeCount = 3
	case clustercomplexity.Large:
		masterCount = 5
		privateNodeCount = 3
	}
	return 0, masterCount, privateNodeCount, nil
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
		MinRAMSize:  7.0,
		MaxRAMSize:  16.0,
		MinDiskSize: 80,
		MinGPU:      -1,
	}
}

func nodeSizing(task concurrency.Task, f flavors.Foreman) abstracts.SizingRequirements {
	return abstracts.SizingRequirements{
		MinCores:    4,
		MaxCores:    8,
		MinRAMSize:  7.0,
		MaxRAMSize:  16.0,
		MinDiskSize: 80,
		MinGPU:      -1,
	}
}

func defaultImage(task concurrency.Task, f flavors.Foreman) string {
	return "Ubuntu 18.04"
}

// getTemplateBox
func getTemplateBox() (*rice.Box, error) {
	anon := templateBox.Load()
	if anon == nil {
		// Note: path MUST be literal for rice to work
		b, err := rice.FindBox("../swarm/scripts")
		if err != nil {
			return nil, err
		}
		templateBox.Store(b)
		anon = templateBox.Load()
	}
	return anon.(*rice.Box), nil
}

// getGlobalSystemRequirements returns the string corresponding to the script swarm_install_requirements.sh
// which installs common features
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
		identity, err := cluster.Identity(task)
		if err != nil {
			return "", err
		}

		// get file contents as string
		tmplString, err := box.String("swarm_install_requirements.sh")
		if err != nil {
			return "", fmt.Errorf("error loading script template: %s", err.Error())
		}

		// parse then execute the template
		tmplPrepared, err := txttmpl.New("install_requirements").Parse(tmplString)
		if err != nil {
			return "", fmt.Errorf("error parsing script template: %s", err.Error())
		}
		dataBuffer := bytes.NewBufferString("")
		data := map[string]interface{}{
			"CIDR":          netCfg.CIDR,
			"CladmPassword": identity.AdminPassword,
			"SSHPublicKey":  identity.Keypair.PublicKey,
			"SSHPrivateKey": identity.Keypair.PrivateKey,
		}
		err = tmplPrepared.Execute(dataBuffer, data)
		if err != nil {
			return "", fmt.Errorf("error realizing script template: %s", err.Error())
		}
		globalSystemRequirementsContent.Store(dataBuffer.String())
		anon = globalSystemRequirementsContent.Load()
	}
	return anon.(string), nil
}

func getNodeInstallationScript(task concurrency.Task, f flavors.Foreman, hostType clusternodetype.Enum) (string, data.Map) {
	script := ""
	data := data.Map{}

	switch hostType {
	case clusternodetype.Gateway:
		script = "swarm_install_gateway.sh"
	case clusternodetype.Master:
		script = "swarm_install_master.sh"
	case clusternodetype.Node:
		script = "swarm_install_node.sh"
	}
	return script, data
}
