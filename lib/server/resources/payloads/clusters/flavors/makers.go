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

package flavors

import (
	rice "github.com/GeertJohan/go.rice"

	"github.com/CS-SI/SafeScale/lib/server/resources/abstracts"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clusternodetype"
	"github.com/CS-SI/SafeScale/lib/server/resources/enums/clusterstate"
	"github.com/CS-SI/SafeScale/lib/utils/concurrency"
	"github.com/CS-SI/SafeScale/lib/utils/data"
)

// Makers ...
type Makers struct {
	MinimumRequiredServers      func(task concurrency.Task, f Foreman) (uint, uint, uint, error)   // returns masterCount, privateNodeCount, publicNodeCount
	DefaultGatewaySizing        func(task concurrency.Task, f Foreman) abstracts.SizingRequirements // sizing of Gateway(s)
	DefaultMasterSizing         func(task concurrency.Task, f Foreman) abstracts.SizingRequirements // default sizing of master(s)
	DefaultNodeSizing           func(task concurrency.Task, f Foreman) abstracts.SizingRequirements // default sizing of node(s)
	DefaultImage                func(task concurrency.Task, f Foreman) string                      // default image of server(s)
	GetNodeInstallationScript   func(task concurrency.Task, f Foreman, nodeType clusternodetype.Enum) (string, data.Map)
	GetGlobalSystemRequirements func(task concurrency.Task, f Foreman) (string, error)
	GetTemplateBox              func() (*rice.Box, error)
	ConfigureGateway            func(task concurrency.Task, f Foreman) error
	CreateMaster                func(task concurrency.Task, f Foreman, index uint) error
	ConfigureMaster             func(task concurrency.Task, f Foreman, index uint, host *abstracts.Host) error
	UnconfigureMaster           func(task concurrency.Task, f Foreman, pbHost *abstracts.Host) error
	CreateNode                  func(task concurrency.Task, f Foreman, index uint, host *abstracts.Host) error
	ConfigureNode               func(task concurrency.Task, f Foreman, index uint, host *abstracts.Host) error
	UnconfigureNode             func(task concurrency.Task, f Foreman, host *abstracts.Host, selectedMasterID string) error
	ConfigureCluster            func(task concurrency.Task, f Foreman) error
	UnconfigureCluster          func(task concurrency.Task, f Foreman) error
	JoinMasterToCluster         func(task concurrency.Task, f Foreman, host *abstracts.Host) error
	JoinNodeToCluster           func(task concurrency.Task, f Foreman, host *abstracts.Host) error
	LeaveMasterFromCluster      func(task concurrency.Task, f Foreman, host *abstracts.Host) error
	LeaveNodeFromCluster        func(task concurrency.Task, f Foreman, host *abstracts.Host, selectedMaster string) error
	GetState                    func(task concurrency.Task, f Foreman) (clusterstate.Enum, error)
}
