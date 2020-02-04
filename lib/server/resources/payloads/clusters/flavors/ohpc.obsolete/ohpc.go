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

package ohpc

/*
 * Implements a cluster of hosts without cluster management environment
 */

import (
	"bytes"
	"fmt"
	"sync/atomic"
	txttmpl "text/template"

	// log "github.com/sirupsen/logrus"
	rice "github.com/GeertJohan/go.rice"

	"github.com/CS-SI/SafeScale/lib/server/resources/abstracts"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clustercomplexity"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clusternodetype"
	"github.com/CS-SI/SafeScale/lib/server/resources/payloads/clusters/flavors"
	"github.com/CS-SI/SafeScale/lib/server/resources/payloads/clusters/flavors/ohpc/enums/ErrorCode"
	"github.com/CS-SI/SafeScale/lib/server/resources/payloads/features"
	"github.com/CS-SI/SafeScale/lib/utils/concurrency"
	"github.com/CS-SI/SafeScale/lib/utils/data"
	"github.com/CS-SI/SafeScale/lib/utils/template"
)

//go:generate rice embed-go

const (
	centos = "CentOS 7.4"
)

var (
	// templateBox is the rice box to use in this package
	templateBox atomic.Value

	// funcMap defines the custome functions to be used in templates
	funcMap = txttmpl.FuncMap{
		// The name "inc" is what the function will be called in the template text.
		"inc": func(i int) int {
			return i + 1
		},
		"errcode": func(msg string) int {
			if code, ok := ErrorCode.StringMap[msg]; ok {
				return int(code)
			}
			return 1023
		},
	}

	globalSystemRequirementsContent atomic.Value

	// Makers initializes a flavors.Makers struct to construct a BOH Cluster
	Makers = flavors.Makers{
		MinimumRequiredServers:      minimumRequiredServers,
		DefaultGatewaySizing:        gatewaySizing,
		DefaultMasterSizing:         nodeSizing,
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

	var privateNodeCount uint
	switch complexity {
	case clustercomplexity.Small:
		privateNodeCount = 1
	case clustercomplexity.Normal:
		privateNodeCount = 3
	case clustercomplexity.Large:
		privateNodeCount = 7
	}

	return 1, privateNodeCount, 0, nil
}

func gatewaySizing(task concurrency.Task, foreman flavors.Foreman) abstracts.SizingRequirements {
	return abstracts.SizingRequirements{
		MinCores:    2,
		MaxCores:    4,
		MinRAMSize:  7.0,
		MaxRAMSize:  16.0,
		MinDiskSize: 50,
		MinGPU:      -1,
	}
}

func nodeSizing(task concurrency.Task, foreman flavors.Foreman) abstracts.SizingRequirements {
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
	return centos
}

func configureCluster(task concurrency.Task, f flavors.Foreman) error {
	// Install feature ohpc-slurm-master on cluster...
	feature, err := features.NewFeature(task, "ohpc-slurm-master")
	if err != nil {
		return err
	}
	cluster := f.Cluster()

	list, err := cluster.ListMasterIPs(task)
	if err != nil {
		return err
	}
	listValues := list.Values()
	values := data.Map{
		"PrimaryMasterIP":   listValues[0],
		"SecondaryMasterIP": "",
	}
	if len(list) > 1 {
		values["SecondaryMasterIP"] = listValues[1]
	}
	results, err := feature.Add(cluster, values, features.Settings{})
	if err != nil {
		return err
	}
	if !results.Successful() {
		return fmt.Errorf(results.AllErrorMessages())
	}

	// Install feature ohpc-slurm-node on cluster...
	feature, err = features.NewFeature(task, "ohpc-slurm-node")
	if err != nil {
		return err
	}
	results, err = feature.Add(cluster, values, features.Settings{})
	if err != nil {
		return err
	}
	if !results.Successful() {
		return fmt.Errorf(results.AllErrorMessages())
	}
	return nil
}

func getNodeInstallationScript(task concurrency.Task, foreman flavors.Foreman, nodeType clusternodetype.Enum) (string, data.Map) {
	script := ""
	data := data.Map{}
	switch nodeType {
	case clusternodetype.Master:
		script = "ohpc_install_master.sh"
	case clusternodetype.Node:
		script = "ohpc_install_node.sh"
	}

	return script, data
}

func getTemplateBox() (*rice.Box, error) {
	anon := templateBox.Load()
	if anon == nil {
		// Note: path MUST be literal for rice to work
		b, err := rice.FindBox("../ohpc/scripts")
		if err != nil {
			return nil, err
		}
		templateBox.Store(b)
		anon = templateBox.Load()
	}
	return anon.(*rice.Box), nil
}

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
		tmplString, err := box.String("ohpc_install_requirements.sh")
		if err != nil {
			return "", fmt.Errorf("error loading script template: %s", err.Error())
		}

		// parse then execute the template
		tmplPrepared, err := txttmpl.New("install_requirements").Funcs(template.MergeFuncs(funcMap, false)).Parse(tmplString)
		if err != nil {
			return "", fmt.Errorf("error parsing script template: %s", err.Error())
		}
		dataBuffer := bytes.NewBufferString("")

		err = tmplPrepared.Execute(dataBuffer, map[string]interface{}{
			"CIDR":          netCfg.CIDR,
			"CladmPassword": identity.AdminPassword,
			"SSHPublicKey":  identity.Keypair.PublicKey,
			"SSHPrivateKey": identity.Keypair.PrivateKey,
		})
		if err != nil {
			return "", fmt.Errorf("error realizing script template: %s", err.Error())
		}
		result := dataBuffer.String()
		globalSystemRequirementsContent.Store(&result)
		anon = globalSystemRequirementsContent.Load()
	}
	return anon.(string), nil
}
