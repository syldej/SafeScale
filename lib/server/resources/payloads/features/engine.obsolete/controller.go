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
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/CS-SI/SafeScale/lib/server/iaas"
	"github.com/CS-SI/SafeScale/lib/server/install"
	"github.com/CS-SI/SafeScale/lib/server/install/enums/method"
	"github.com/CS-SI/SafeScale/lib/server/resources"
	"github.com/CS-SI/SafeScale/lib/utils"
	"github.com/CS-SI/SafeScale/lib/utils/concurrency"
	"github.com/CS-SI/SafeScale/lib/utils/data"
	"github.com/CS-SI/SafeScale/lib/utils/scerr"
	"github.com/CS-SI/SafeScale/lib/utils/temporal"
)

var (
	// EmptyValues corresponds to no values for the feature
	EmptyValues = data.Map{}
	// checkCache  = utils.NewMapCache()
)

// feature contains the information about an installable feature
type feature struct {
	// name is the name of the service
	name string
	// filename is the name of the specification file
	filename string
	// displayFilename
	displayFilename string
	// embedded tells if the feature is embedded in deploy
	embedded bool
	// Installers defines the installers available for the feature
	installers map[method.Enum]Installer
	// Dependencies lists other feature(s) (by name) needed by this one
	//dependencies []string
	// Management contains a string map of data that could be used to manage the feature (if it makes sense)
	// This could be used to explain to Service object how to manage the feature, to react as a service
	//Management map[string]interface{}
	// specs is the Viper instance containing feature specification
	specs   *viper.Viper
	task    concurrency.Task
	service iaas.Service
}

// ListFeatures lists all features suitable for hosts or clusters
func ListFeatures(suitableFor string) ([]interface{}, error) {
	features := allEmbeddedMap
	cfgFiles := []interface{}{}

	var paths []string
	paths = append(paths, utils.AbsPathify("$HOME/.safescale/features"))
	paths = append(paths, utils.AbsPathify("$HOME/.config/safescale/features"))
	paths = append(paths, utils.AbsPathify("/etc/safescale/features"))

	errors := []error{}

	for _, path := range paths {
		files, err := ioutil.ReadDir(path)
		if err == nil {
			task, err := concurrency.RootTask()
			if err != nil {
				errors = append(errors, err)
			} else {
				for _, f := range files {
					if isCfgFile := strings.HasSuffix(strings.ToLower(f.Name()), ".yml"); isCfgFile == true {
						feat, err := NewFeature(task, strings.Replace(strings.ToLower(f.Name()), ".yml", "", 1))
						if err != nil {
							logrus.Error(err)
							errors = append(errors, err)
							continue
						}
						if _, ok := allEmbeddedMap[feat.Name()]; !ok {
							allEmbeddedMap[feat.Name()] = feat.(*feature)
						}
					}
				}
			}
		}
	}

	if len(errors) > 0 {
		return nil, scerr.ErrListError(errors)
	}

	for _, feat := range features {
		switch suitableFor {
		case "host":
			yamlKey := "feature.suitableFor.host"
			if feat.Specs().IsSet(yamlKey) {
				value := strings.ToLower(feat.Specs().GetString(yamlKey))
				if value == "ok" || value == "yes" || value == "true" || value == "1" {
					cfgFiles = append(cfgFiles, feat.Filename())
				}
			}
		case "cluster":
			yamlKey := "feature.suitableFor.cluster"
			if feat.Specs().IsSet(yamlKey) {
				values := strings.Split(strings.ToLower(feat.Specs().GetString(yamlKey)), ",")
				if values[0] == "all" || values[0] == "dcos" || values[0] == "k8s" || values[0] == "boh" || values[0] == "swarm" || values[0] == "ohpc" {
					cfg := struct {
						FeatureName    string   `json:"feature"`
						ClusterFlavors []string `json:"available-cluster-flavors"`
					}{feat.Name(), []string{}}

					cfg.ClusterFlavors = append(cfg.ClusterFlavors, values...)

					cfgFiles = append(cfgFiles, cfg)
				}
			}
		default:
			return nil, fmt.Errorf("unknown parameter value : %s \n (should be host or cluster)", suitableFor)
		}

	}

	return cfgFiles, nil
}

// NewFeature searches for a spec file name 'name' and initializes a new Feature object
// with its content
func NewFeature(task concurrency.Task, name string) (_ install.Feature, err error) {
	if name == "" {
		return nil, scerr.InvalidParameterError("name", "cannot be empty string")
	}

	tracer := concurrency.NewTracer(task, "", true).WithStopwatch().GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	v := viper.New()
	v.AddConfigPath(".")
	v.AddConfigPath("$HOME/.safescale/features")
	v.AddConfigPath("$HOME/.config/safescale/features")
	v.AddConfigPath("/etc/safescale/features")
	v.SetConfigName(name)

	var feat *feature
	err = v.ReadInConfig()
	if err != nil {
		switch err.(type) {
		case viper.ConfigFileNotFoundError:
			// Failed to find a spec file on filesystem, trying with embedded ones
			err = nil
			if _, ok := allEmbeddedMap[name]; !ok {
				err = fmt.Errorf("failed to find a feature named '%s'", name)
			} else {
				feat = allEmbeddedMap[name]
				feat.task = task
			}
		default:
			err = fmt.Errorf("failed to read the specification file of feature called '%s': %s", name, err.Error())
		}
	}
	if v.IsSet("feature") {
		feat = &feature{
			filename: name + ".yml",
			name:     name,
			specs:    v,
			task:     task,
		}
		feat.displayFilename = feat.filename
	}
	return feat, err
}

// NewEmbeddedFeature searches for an embedded featured named 'name' and initializes a new Feature object
// with its content
func NewEmbeddedFeature(task concurrency.Task, name string) (_ install.Feature, err error) {
	if task == nil {
		task, _ := concurrency.VoidTask()
	}
	if name == "" {
		return nil, scerr.InvalidParameterError("name", "cannot be empty string")
	}

	tracer := concurrency.NewTracer(task, "", true).WithStopwatch().GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	var feat *feature
	if _, ok := allEmbeddedMap[name]; !ok {
		err = fmt.Errorf("failed to find a feature named '%s'", name)
	} else {
		feat = allEmbeddedMap[name]
		feat.task = task
	}
	return feat, err
}

// Clone makes a full copy of a feature with new task and service, and installers reset
func (f *feature) Clone(task concurrency.Task, svc iaas.Service) (install.Feature, error) {
	if f == nil {
		return nil, scerr.InvalidInstanceError()
	}
	if task == nil {
		return nil, scerr.InvalidParameterError("task", "can't be nil")
	}
	if svc == nil {
		return nil, scerr.InvalidParameterError("svc", "can't be nil)")
	}
	newF := &feature{}
	*newF = *f
	newF.installers = map[method.Enum]Installer{}
	newF.task = task
	newF.service = svc
	return nil, nil
}

// installerOfMethod instanciates the right installer corresponding to the method
func (f *feature) installerOfMethod(method method.Enum) Installer {
	var installer Installer
	switch method {
	case method.Bash:
		installer = NewBashInstaller()
	case method.Apt:
		installer = NewAptInstaller()
	case method.Yum:
		installer = NewYumInstaller()
	case method.Dnf:
		installer = NewDnfInstaller()
	case method.DCOS:
		installer = NewDcosInstaller()
		//	case method.Ansible:
		//		installer = NewAnsibleInstaller()
		//	case method.Helm:
		//		installer = NewHelmInstaller()
	}
	return installer
}

// DisplayName returns the name of the feature
func (f *feature) Name() string {
	return f.name
}

// Filename returns the name of the feature
func (f *feature) Filename() string {
	return f.name
}

// DisplayFilename returns the full file name, with [embedded] added at the end if the
// feature is embedded.
func (f *feature) DisplayFilename() string {
	filename := f.filename
	if f.embedded {
		filename += " [embedded]"
	}
	return filename
}

// Specs returns a copy of the spec file (we don't want external use to modify Feature.specs)
func (f *feature) Specs() *viper.Viper {
	roSpecs := *f.specs
	return &roSpecs
}

// Applyable tells if the feature is installable on the target
func (f *feature) Applyable(t install.Targetable) bool {
	methods := t.Methods(f.task)
	for _, k := range methods {
		installer := f.installerOfMethod(k)
		if installer != nil {
			return true
		}
	}
	return false
}

// Check if feature is installed on target
// Check is ok if error is nil and Results.Successful() is true
func (f *feature) Check(t install.Targetable, v data.Map, s install.Settings) (_ install.Results, err error) {
	if f == nil {
		return nil, scerr.InvalidInstanceError()
	}

	tracer := concurrency.NewTracer(f.task, fmt.Sprintf("(): '%s' on %s '%s'", f.Name(), t.Type(), t.Name()), true).WithStopwatch().GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	// cacheKey := f.DisplayName() + "@" + t.Name()
	// if anon, ok := checkCache.Get(cacheKey); ok {
	// 	return anon.(Results), nil
	// }

	methods := t.Methods(f.task)
	var installer Installer
	for _, method := range methods {
		if f.specs.IsSet(fmt.Sprintf("feature.install.%s", strings.ToLower(method.String()))) {
			installer = f.installerOfMethod(method)
			if installer != nil {
				break
			}
		}
	}
	if installer == nil {
		return nil, fmt.Errorf("failed to find a way to check '%s'", f.Name())
	}

	logrus.Debugf("Checking if feature '%s' is installed on %s '%s'...", f.Name(), t.Type(), t.Name())

	// 'v' may be updated by parallel tasks, so use copy of it
	myV := v.Clone()

	// Inits implicit parameters
	err = f.setImplicitParameters(t, myV)
	if err != nil {
		return nil, err
	}

	// Checks required parameters have value
	err = checkParameters(f, myV)
	if err != nil {
		return nil, err
	}

	results, err := installer.Check(f, t, myV, s)
	// _ = checkCache.ForceSet(cacheKey, results)
	return results, err
}

// Add installs the feature on the target
// Installs succeeds if error == nil and Results.Successful() is true
func (f *feature) Add(t install.Targetable, v data.Map, s install.Settings) (_ install.Results, err error) {
	if f == nil {
		return nil, scerr.InvalidInstanceError()
	}

	tracer := concurrency.NewTracer(f.task, fmt.Sprintf("(): '%s' on %s '%s'", f.Name(), t.Type(), t.Name()), true).WithStopwatch().GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	methods := t.Methods(f.task)
	var (
		installer Installer
		i         uint8
	)
	for i = 1; i <= uint8(len(methods)); i++ {
		method := methods[i]
		if f.specs.IsSet(fmt.Sprintf("feature.install.%s", strings.ToLower(method.String()))) {
			installer = f.installerOfMethod(method)
			if installer != nil {
				break
			}
		}
	}
	if installer == nil {
		return nil, fmt.Errorf("failed to find a way to install '%s'", f.Name())
	}

	defer temporal.NewStopwatch().OnExitLogInfo(
		fmt.Sprintf("Starting addition of feature '%s' on %s '%s'...", f.Name(), t.Type(), t.Name()),
		fmt.Sprintf("Ending addition of feature '%s' on %s '%s'", f.Name(), t.Type(), t.Name()),
	)()

	// 'v' may be updated by parallel tasks, so use copy of it
	myV := v.Clone()

	// Inits implicit parameters
	err = f.setImplicitParameters(t, myV)
	if err != nil {
		return nil, err
	}

	// Checks required parameters have value
	err = checkParameters(f, myV)
	if err != nil {
		return nil, err
	}

	if !s.AddUnconditionally {
		res, err := f.Check(t, v, s)
		if err != nil {
			return nil, fmt.Errorf("failed to check feature '%s': %s", f.Name(), err.Error())
		}
		if res.Successful() {
			logrus.Infof("Feature '%s' is already installed.", f.Name())
			return res, nil
		}
	}

	if !s.SkipFeatureRequirements {
		err := f.installRequirements(t, v, s)
		if err != nil {
			return nil, fmt.Errorf("failed to install requirements: %s", err.Error())
		}
	}
	res, err := installer.Add(f, t, myV, s)
	if err == nil {
		// _ = checkCache.ForceSet(f.DisplayName()+"@"+t.Name(), results)
		return nil, err
	}

	return res, err
}

// Remove uninstalls the feature from the target
func (f *feature) Remove(t install.Targetable, v data.Map, s install.Settings) (_ install.Results, err error) {
	if f == nil {
		return nil, scerr.InvalidInstanceError()
	}

	tracer := concurrency.NewTracer(f.task, fmt.Sprintf("(): '%s' on %s '%s'", f.Name(), t.Type(), t.Name()), true).WithStopwatch().GoingIn()
	defer tracer.OnExitTrace()()
	defer scerr.OnExitLogError(tracer.TraceMessage(""), &err)()

	var res install.Results
	methods := t.Methods(f.task)
	var installer Installer
	for _, method := range methods {
		if f.specs.IsSet(fmt.Sprintf("feature.install.%s", strings.ToLower(method.String()))) {
			installer = f.installerOfMethod(method)
			if installer != nil {
				break
			}
		}
	}
	if installer == nil {
		return nil, fmt.Errorf("failed to find a way to uninstall '%s'", f.Name())
	}

	defer temporal.NewStopwatch().OnExitLogInfo(
		fmt.Sprintf("Starting removal of feature '%s' from %s '%s'", f.Name(), t.Type(), t.Name()),
		fmt.Sprintf("Ending removal of feature '%s' from %s '%s'", f.Name(), t.Type(), t.Name()),
	)()

	// 'v' may be updated by parallel tasks, so use copy of it
	myV := v.Clone()

	// Inits implicit parameters
	err = f.setImplicitParameters(t, myV)
	if err != nil {
		return nil, err
	}

	// Checks required parameters have value
	err = checkParameters(f, myV)
	if err != nil {
		return nil, err
	}

	res, err = installer.Remove(f, t, myV, s)
	// checkCache.Reset(f.DisplayName() + "@" + t.Name())
	return res, err
}

// installRequirements walks through requirements and installs them if needed
func (f *feature) installRequirements(t install.Targetable, v data.Map, s install.Settings) error {
	yamlKey := "feature.requirements.features"
	if f.specs.IsSet(yamlKey) {
		{
			msgHead := fmt.Sprintf("Checking requirements of feature '%s'", f.Name())
			msgTail := fmt.Sprintf("on %s '%s'", t.Type(), t.Name())
			logrus.Debugf("%s %s...", msgHead, msgTail)
		}
		for _, requirement := range f.specs.GetStringSlice(yamlKey) {
			needed, err := NewFeature(f.task, requirement)
			if err != nil {
				return fmt.Errorf("failed to find required feature '%s': %s", requirement, err.Error())
			}
			results, err := needed.Check(t, v, s)
			if err != nil {
				return fmt.Errorf("failed to check required feature '%s' for feature '%s': %s", requirement, f.Name(), err.Error())
			}
			if !results.Successful() {
				results, err := needed.Add(t, v, s)
				if err != nil {
					return fmt.Errorf("failed to install required feature '%s': %s", requirement, err.Error())
				}
				if !results.Successful() {
					return fmt.Errorf("failed to install required feature '%s':\n%s", requirement, results.AllErrorMessages())
				}
			}
		}
	}
	return nil
}

// setImplicitParameters configures parameters that are implicitly defined, based on target
func (f *feature) setImplicitParameters(t install.Targetable, v data.Map) error {
	if t.Type() == "cluster" {
		cluster, ok := t.(resources.Cluster)
		if !ok {
			return scerr.InconsistentError("'resources.Cluster' expected, '%s' provided", reflect.TypeOf(t).String())
		}
		identity, err := cluster.Identity(f.task)
		if err != nil {
			return err
		}

		v["ClusterName"] = identity.Name
		v["ClusterComplexity"] = strings.ToLower(identity.Complexity.String())
		v["ClusterFlavor"] = strings.ToLower(identity.Flavor.String())
		networkCfg, err := cluster.NetworkConfig(f.task)
		if err != nil {
			return err
		}
		v["PrimaryGatewayIP"] = networkCfg.GatewayIP
		// v["SecondaryGatewayIP"] = networkCfg.SecondaryGatewayIP
		// v["DefaultRouteIP"] = networkCfg.DefaultRouteIP
		v["GatewayIP"] = v["DefaultRouteIP"] // legacy ...
		// v["PrimaryPublicIP"] = networkCfg.PrimaryPublicIP
		// v["SecondaryPublicIP"] = networkCfg.SecondaryPublicIP
		// v["EndpointIP"] = networkCfg.EndpointIP
		v["PublicIP"] = v["EndpointIP"] // legacy ...
		if _, ok := v["CIDR"]; !ok {
			v["CIDR"] = networkCfg.CIDR
		}

		list, err := cluster.ListMasters(f.task)
		if err != nil {
			return err
		}
		v["Masters"] = list

		listMap, err := cluster.ListMasterIDs(f.task)
		if err != nil {
			return err
		}
		keys, values := extractKeysAndValuesFromMap(listMap)
		v["MasterNumericalIDs"] = keys
		v["MasterIDs"] = values

		listMap, err = cluster.ListMasterIDs(f.task)
		if err != nil {
			return err
		}
		_, values = extractKeysAndValuesFromMap(listMap)
		v["MasterNames"] = values

		listMap, err = cluster.ListMasterIPs(f.task)
		if err != nil {
			return err
		}
		_, values = extractKeysAndValuesFromMap(listMap)
		v["MasterIPs"] = values

		v["ClusterAdminUsername"] = "cladm"
		v["ClusterAdminPassword"] = identity.AdminPassword
	} else {
		host, ok := t.(resources.Host)
		if !ok {
			return scerr.InconsistentError("'resources.Host' expected, '%s' provided", reflect.TypeOf(t).String())
		}

		// v["Hostname"] = host.Name
		// v["HostIP"] = host.PrivateIp
		// FIXME:
		objn, err := host.DefaultNetwork(f.task)
		if err != nil {
			return err
		}

		objpgw, err := objn.Gateway(f.task, true)
		if err != nil {
			return err
		}
		v["GatewayIP"], err = objpgw.PrivateIP(f.task)
		if err != nil {
			return err
		}

		objsgw, err := objn.Gateway(f.task, false)
		if err != nil {
			if _, ok := err.(scerr.ErrNotFound); !ok {
				return err
			}
		} else {
			v["SecondaryGatewayIP"], err = objsgw.PrivateIP(f.task)
			if err != nil {
				return err
			}
		}
		v["DefaultRouteIP"], err = objn.DefaultRouteIP(f.task)
		if err != nil {
			return err
		}
		v["EndpointIP"], err = objn.EndpointIP(f.task)
		if err != nil {
			return err
		}

		if _, ok := v["Username"]; !ok {
			v["Username"] = "safescale"
		}
	}

	return nil
}

// extractKeysAndValuesFromMap returns a slice with keys and a slice with values from map[uint]string
func extractKeysAndValuesFromMap(m cluster.NodeStringList) ([]uint, []string) {
	length := len(m)
	if length <= 0 {
		return []uint{}, []string{}
	}

	keys := make([]uint, 0, length)
	values := make([]string, 0, length)
	for k, v := range m {
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values
}
