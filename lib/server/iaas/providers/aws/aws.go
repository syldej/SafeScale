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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or provideried.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package aws

import (
	"fmt"

	"github.com/CS-SI/SafeScale/lib/server/iaas"
	"github.com/CS-SI/SafeScale/lib/server/iaas/objectstorage"
	"github.com/CS-SI/SafeScale/lib/server/iaas/providers"
	apiprovider "github.com/CS-SI/SafeScale/lib/server/iaas/providers/api"
	"github.com/CS-SI/SafeScale/lib/server/iaas/resources"
	"github.com/CS-SI/SafeScale/lib/server/iaas/resources/enums/VolumeSpeed"
	"github.com/CS-SI/SafeScale/lib/server/iaas/stacks"
	"github.com/CS-SI/SafeScale/lib/server/iaas/stacks/aws"
)

// provider is the provider implementation of the Gcp provider
type provider struct {
	*aws.Stack
}

// New creates a new instance of gcp provider
func New() apiprovider.Provider {
	return &provider{}
}

// Build build a new Client from configuration parameter
func (p *provider) Build(params map[string]interface{}) (apiprovider.Provider, error) {
	// tenantName, _ := params["name"].(string)

	identityCfg, ok := params["identity"].(map[string]interface{})
	if !ok {
		return &provider{}, fmt.Errorf("section identity not found in tenants.toml")
	}

	computeCfg, ok := params["compute"].(map[string]interface{})
	if !ok {
		return &provider{}, fmt.Errorf("section compute not found in tenants.toml")
	}

	// FIXME Use the network to change default "safescale" network
	// network, _ := params["network"].(map[string]interface{})

	region, _ := computeCfg["Region"].(string)
	zone, _ := computeCfg["Zone"].(string)

	s3Endpoint, _ := computeCfg["S3"].(string)
	ec2Endpoint, _ := computeCfg["EC2"].(string)
	ssmEndpoint, _ := computeCfg["SSM"].(string)

	awsConf := stacks.AWSConfiguration{
		S3Endpoint:  s3Endpoint,
		Ec2Endpoint: ec2Endpoint,
		SsmEndpoint: ssmEndpoint,
		Region:      region,
		Zone:        zone,
	}

	username, ok := identityCfg["Username"].(string)
	if !ok || username == "" {
		username, _ = identityCfg["User"].(string)
	}
	password, _ := identityCfg["Password"].(string)
	secretKey, _ := identityCfg["SecretKey"].(string)
	identityEndpoint, _ := identityCfg["auth_uri"].(string)

	projectName, _ := computeCfg["ProjectName"].(string)
	projectID, _ := computeCfg["ProjectID"].(string)
	defaultImage, _ := computeCfg["DefaultImage"].(string)

	operatorUsername := resources.DefaultUser
	if operatorUsernameIf, ok := computeCfg["OperatorUsername"]; ok {
		operatorUsername = operatorUsernameIf.(string)
	}

	// FIXME Use authentication options
	authOptions := stacks.AuthenticationOptions{
		IdentityEndpoint: identityEndpoint,
		Username:         username,
		Password:         password,
		SecretAccessKey:  secretKey,
		Region:           region,
		ProjectName:      projectName,
		ProjectID:        projectID,
		FloatingIPPool:   "public",
	}

	metadataBucketName, err := objectstorage.BuildMetadataBucketName("aws", region, "", projectID)
	if err != nil {
		return nil, err
	}

	cfgOptions := stacks.ConfigurationOptions{
		DNSList:                   []string{"8.8.8.8", "1.1.1.1"},
		UseFloatingIP:             true,
		AutoHostNetworkInterfaces: false,
		VolumeSpeeds: map[string]VolumeSpeed.Enum{
			"standard":   VolumeSpeed.COLD,
			"performant": VolumeSpeed.HDD,
		},
		MetadataBucket:   metadataBucketName,
		DefaultImage:     defaultImage,
		OperatorUsername: operatorUsername,
		UseNATService:    true,
	}

	stack, err := aws.New(authOptions, awsConf, cfgOptions)
	if err != nil {
		return nil, err
	}

	providerName := "aws"

	evalid := apiprovider.NewValidatedProvider(&provider{stack}, providerName)
	etrace := apiprovider.NewErrorTraceProvider(evalid, providerName)
	prov := apiprovider.NewLoggedProvider(etrace, providerName)

	return prov, nil
}

// GetAuthenticationOptions returns the auth options
func (p *provider) GetAuthenticationOptions() (providers.Config, error) {
	cfg := providers.ConfigMap{}

	opts := p.Stack.GetAuthenticationOptions()
	cfg.Set("TenantName", opts.TenantName)
	cfg.Set("Login", opts.Username)
	cfg.Set("Password", opts.Password)
	cfg.Set("AuthUrl", opts.IdentityEndpoint)
	cfg.Set("Region", opts.Region)
	return cfg, nil
}

// GetConfigurationOptions return configuration parameters
func (p *provider) GetConfigurationOptions() (providers.Config, error) {
	cfg := providers.ConfigMap{}

	opts := p.Stack.GetConfigurationOptions()
	cfg.Set("DNSList", opts.DNSList)
	cfg.Set("AutoHostNetworkInterfaces", opts.AutoHostNetworkInterfaces)
	cfg.Set("UseLayer3Networking", opts.UseLayer3Networking)
	cfg.Set("DefaultImage", opts.DefaultImage)
	cfg.Set("MetadataBucketName", opts.MetadataBucket)
	cfg.Set("OperatorUsername", opts.OperatorUsername)
	return cfg, nil
}

// GetName returns the providerName
func (p *provider) GetName() string {
	return "aws"
}

// ListImages ...
func (p *provider) ListImages(all bool) ([]resources.Image, error) {
	return p.Stack.ListImages()
}

func (p *provider) ListTemplates(all bool) ([]resources.HostTemplate, error) {
	return p.Stack.ListTemplates()
}

func init() {
	iaas.Register("aws", &provider{})
}
