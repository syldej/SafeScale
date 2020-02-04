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

package ErrorCode

import (
	"fmt"
	"strings"
)

type Enum int

const (
	_ Enum = iota

	DockerComposeConfig
	DockerComposeExecution
	SystemUpdate
	ToolsInstall

	//NextErrorCode is the next error code usable
	NextErrorCode
)

var (
	StringMap = map[string]Enum{
		"DockerComposeConfig":    DockerComposeConfig,
		"DockerComposeExecution": DockerComposeExecution,
		"SystemUpdate":           SystemUpdate,
		"ToolsInstall":           ToolsInstall,
	}

	enumMap = map[Enum]string{
		DockerComposeConfig:    "DockerComposeConfig",
		DockerComposeExecution: "DockerComposeExecution",
		SystemUpdate:           "SystemUpdate",
		ToolsInstall:           "ToolsInstall",
	}
)

// Parse returns a Enum corresponding to the string parameter
// If the string doesn't correspond to any Enum, returns an error (nil otherwise)
// This function is intended to be used to parse user input.
func Parse(v string) (Enum, error) {
	var (
		e  Enum
		ok bool
	)
	lowered := strings.ToLower(v)
	if e, ok = StringMap[lowered]; !ok {
		return e, fmt.Errorf("failed to find a Flavor matching with '%s'", v)
	}
	return e, nil

}

// String returns a string representation of an Enum
func (e Enum) String() string {
	if str, found := enumMap[e]; found {
		return str
	}
	return ""
}
