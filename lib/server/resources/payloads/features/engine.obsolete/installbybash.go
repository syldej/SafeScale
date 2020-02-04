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

package engine

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/CS-SI/SafeScale/lib/server/install"
	"github.com/CS-SI/SafeScale/lib/server/install/enums/action"
	"github.com/CS-SI/SafeScale/lib/server/install/enums/method"
	"github.com/CS-SI/SafeScale/lib/utils/data"
)

// bashInstaller is an installer using script to add and remove a feature
type bashInstaller struct{}

func (i *bashInstaller) GetName() string {
	return "script"
}

// Check checks if the feature is installed, using the check script in Specs
func (i *bashInstaller) Check(f install.Feature, t install.Targetable, v data.Map, s install.Settings) (install.Results, error) {
	yamlKey := "feature.install.bash.check"
	if !f.Specs().IsSet(yamlKey) {
		msg := `syntax error in feature '%s' specification file (%s): no key '%s' found`
		return nil, fmt.Errorf(msg, f.Name(), f.DisplayFilename(), yamlKey)
	}

	worker, err := newWorker(f.(*feature), t, method.Bash, action.Check, nil)
	if err != nil {
		return nil, err
	}

	err = worker.CanProceed(s)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	return worker.Proceed(v, s)
}

// Add installs the feature using the install script in Specs
// 'values' contains the values associated with parameters as defined in specification file
func (i *bashInstaller) Add(f install.Feature, t install.Targetable, v data.Map, s install.Settings) (install.Results, error) {
	// Determining if install script is defined in specification file
	if !f.Specs().IsSet("feature.install.bash.add") {
		msg := `syntax error in feature '%s' specification file (%s):
				no key 'feature.install.bash.add' found`
		return nil, fmt.Errorf(msg, f.Name(), f.DisplayFilename())
	}

	worker, err := newWorker(f.(*feature), t, method.Bash, action.Add, nil)
	if err != nil {
		return nil, err
	}
	err = worker.CanProceed(s)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	if !worker.ConcernsCluster() {
		if _, ok := v["Username"]; !ok {
			v["Username"] = "safescale"
		}
	}
	return worker.Proceed(v, s)
}

// Remove uninstalls the feature
func (i *bashInstaller) Remove(f install.Feature, t install.Targetable, v data.Map, s install.Settings) (install.Results, error) {
	if !f.Specs().IsSet("feature.install.bash.remove") {
		msg := `syntax error in feature '%s' specification file (%s):
				no key 'feature.install.bash.remove' found`
		return nil, fmt.Errorf(msg, f.Name(), f.DisplayFilename())
	}

	worker, err := newWorker(f.(*feature), t, method.Bash, action.Remove, nil)
	if err != nil {
		return nil, err
	}
	err = worker.CanProceed(s)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	if t.Type() != "cluster" {
		if _, ok := v["Username"]; !ok {
			v["Username"] = "safescale"
		}
	}
	return worker.Proceed(v, s)
}

// NewBashInstaller creates a new instance of Installer using script
func NewBashInstaller() Installer {
	return &bashInstaller{}
}
