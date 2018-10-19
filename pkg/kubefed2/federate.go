/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubefed2

import (
	"fmt"
	"io"

	"github.com/golang/glog"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	apiextv1b1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextv1b1client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apicommon "github.com/kubernetes-sigs/federation-v2/pkg/apis/core/common"
	"github.com/kubernetes-sigs/federation-v2/pkg/apis/core/typeconfig"
	fedv1a1 "github.com/kubernetes-sigs/federation-v2/pkg/apis/core/v1alpha1"
	controllerutil "github.com/kubernetes-sigs/federation-v2/pkg/controller/util"
	"github.com/kubernetes-sigs/federation-v2/pkg/kubefed2/options"
	"github.com/kubernetes-sigs/federation-v2/pkg/kubefed2/util"
)

var (
	federate_long = `
	Federate enables a Kubernetes API type (including a CRD) to be
	propagated to members of a federation.  Federation primitives will be
	generated as CRDs and a FederatedTypeConfig will be created to
	configure a sync controller.

    Current context is assumed to be a Kubernetes cluster
    hosting the federation control plane. Please use the
    --host-cluster-context flag otherwise.`

	federate_example = `
	# Enable federation of a Kubernetes type by specifying the
	# Kind, version and group (optional for core types) of the
	# target type. The context of the federation control plane's
	# host cluster must be supplied if it is not the current
	# context.
	kubefed2 federate --group= --version=v1 --kind=Secret --host-cluster-context=bar`
)

type federateType struct {
	options.SubcommandOptions
	federateTypeOptions
}

type federateTypeOptions struct {
	group              string
	version            string
	kind               string
	pluralName         string
	namespaced         bool // TODO(marun) Discover this from the openapi spec
	rawComparisonField string
	comparisonField    apicommon.VersionComparisonField
	templateVersion    string
	templateGroup      string
}

// Bind adds the join specific arguments to the flagset passed in as an
// argument.
func (o *federateTypeOptions) Bind(flags *pflag.FlagSet) {
	flags.StringVar(&o.group, "group", "", "Name of the API group of the target API type.")
	flags.StringVar(&o.version, "version", "", "The API version of the target API type.")
	flags.StringVar(&o.kind, "kind", "", "The API kind of the target API type.")
	flags.StringVar(&o.pluralName, "plural-name", "",
		"Lower-case plural name of the target API type.  Defaults to the kind lower-cased and pluralized.")
	flags.BoolVar(&o.namespaced, "namespaced", true, "Whether the target kind is namespaced.")
	flags.StringVar(&o.rawComparisonField, "comparison-field", string(apicommon.ResourceVersionField),
		fmt.Sprintf("The field in the target type to compare for equality. Valid values are %q (default) and %q.",
			apicommon.ResourceVersionField, apicommon.GenerationField,
		),
	)
	flags.StringVar(&o.templateGroup, "template-group", "generated.federation.k8s.io", "The name of the API group of the target API type.")
	flags.StringVar(&o.templateVersion, "template-version", "v1alpha1", "The API version of the target API type.")
	// TODO(marun) Consider supporting custom names for template/placement/override
}

// NewCmdFederate defines the `federate` command that enables
// federation of a Kubernetes API type.
func NewCmdFederate(cmdOut io.Writer, config util.FedConfig) *cobra.Command {
	opts := &federateType{}

	cmd := &cobra.Command{
		Use:     "federate --group=GROUP --version=VERSION --kind=KIND --host-cluster-context=HOST_CONTEXT",
		Short:   "Enable propagation of a Kubernetes API type",
		Long:    federate_long,
		Example: federate_example,
		Run: func(cmd *cobra.Command, args []string) {
			err := opts.Complete(args)
			if err != nil {
				glog.Fatalf("error: %v", err)
			}

			err = opts.Run(cmdOut, config)
			if err != nil {
				glog.Fatalf("error: %v", err)
			}
		},
	}

	flags := cmd.Flags()
	opts.CommonBind(flags)
	opts.Bind(flags)

	return cmd
}

// Complete ensures that options are valid and marshals them if necessary.
func (j *federateType) Complete(args []string) error {
	if len(j.group) == 0 && j.version != "v1" {
		return fmt.Errorf("--group is a mandatory parameter for non-core resources ")
	}
	if len(j.version) == 0 {
		return fmt.Errorf("--version is a mandatory parameter")
	}
	if len(j.kind) == 0 {
		return fmt.Errorf("--kind is a mandatory parameter")
	}
	if len(j.templateGroup) == 0 {
		return fmt.Errorf("--template-group is a mandatory parameter ")
	}
	if len(j.templateVersion) == 0 {
		return fmt.Errorf("--template-version is a mandatory parameter")
	}
	if j.rawComparisonField == string(apicommon.ResourceVersionField) ||
		j.rawComparisonField == string(apicommon.GenerationField) {
		j.comparisonField = apicommon.VersionComparisonField(j.rawComparisonField)
	} else {
		return fmt.Errorf("--comparison-field must be %q or %q",
			apicommon.ResourceVersionField, apicommon.GenerationField,
		)
	}
	if len(j.pluralName) == 0 {
		j.pluralName = fedv1a1.PluralName(j.kind)
	}

	return nil
}

// Run is the implementation of the `federate` command.
func (j *federateType) Run(cmdOut io.Writer, config util.FedConfig) error {
	hostConfig, err := config.HostConfig(j.HostClusterContext, j.Kubeconfig)
	if err != nil {
		return fmt.Errorf("Failed to get host cluster config: %v", err)
	}

	fedClient, err := util.FedClientset(hostConfig)
	if err != nil {
		return fmt.Errorf("Failed to get federation clientset: %v", err)
	}

	crdClient, err := apiextv1b1client.NewForConfig(hostConfig)
	if err != nil {
		return fmt.Errorf("Failed to create crd clientset: %v", err)
	}

	apiResource := metav1.APIResource{
		Group:      j.group,
		Version:    j.version,
		Kind:       j.kind,
		Name:       j.pluralName,
		Namespaced: j.namespaced,
	}

	// TODO(marun) Perform this check with the discovery client.
	targetClient, err := controllerutil.NewResourceClientFromConfig(hostConfig, &apiResource)
	if err != nil {
		return fmt.Errorf("Error creating client for target resource %q: %v", apiResource.Kind, err)
	}
	_, err = targetClient.Resources(j.FederationNamespace).List(metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("Unable to verify that %q is a valid target type: %v", apiResource.Kind, err)
	}

	typeConfig := TypeConfigForTarget(apiResource, j.comparisonField, j.templateGroup, j.templateVersion)

	if j.DryRun {
		// Avoid mutating the API
		return nil
	}

	// TODO(marun) Retrieve the validation schema of the target and
	// use it in constructing the schema for the template.
	err = CreatePrimitives(crdClient, typeConfig)
	if err != nil {
		return err
	}

	concreteTypeConfig := typeConfig.(*fedv1a1.FederatedTypeConfig)
	_, err = fedClient.CoreV1alpha1().FederatedTypeConfigs(j.FederationNamespace).Create(concreteTypeConfig)
	if err != nil {
		return fmt.Errorf("Error creating FederatedTypeConfig %q: %v", concreteTypeConfig.Name, err)
	}

	return nil
}

func CreatePrimitives(client *apiextv1b1client.ApiextensionsV1beta1Client, typeConfig typeconfig.Interface) error {
	for _, apiResource := range []metav1.APIResource{
		typeConfig.GetTemplate(),
		typeConfig.GetPlacement(),
		// TODO(marun) Optionally support overrides
	} {
		_, err := CreateCrdFromResource(client, apiResource)
		if err != nil {
			return err
		}
	}
	return nil
}

func CreateCrdFromResource(client *apiextv1b1client.ApiextensionsV1beta1Client, apiResource metav1.APIResource) (*apiextv1b1.CustomResourceDefinition, error) {
	crd := CrdForAPIResource(apiResource)
	createdCrd, err := client.CustomResourceDefinitions().Create(crd)
	if err != nil {
		return nil, fmt.Errorf("Error creating CRD %q: %v", crd.Name, err)
	}
	return createdCrd, nil
}

func TypeConfigForTarget(apiResource metav1.APIResource, comparisonField apicommon.VersionComparisonField, templateGroup, templateVersion string) typeconfig.Interface {
	kind := apiResource.Kind
	typeConfig := &fedv1a1.FederatedTypeConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: groupQualifiedName(apiResource),
		},
		Spec: fedv1a1.FederatedTypeConfigSpec{
			Target: fedv1a1.APIResource{
				Version: apiResource.Version,
				Kind:    kind,
			},
			Namespaced:         apiResource.Namespaced,
			ComparisonField:    comparisonField,
			PropagationEnabled: true,
			Template: fedv1a1.APIResource{
				Group:   templateGroup,
				Version: templateVersion,
				Kind:    fmt.Sprintf("Federated%s", kind),
			},
			Placement: fedv1a1.APIResource{
				Kind: fmt.Sprintf("Federated%sPlacement", kind),
			},
		},
	}
	// Set defaults that would normally be set by the api
	fedv1a1.SetFederatedTypeConfigDefaults(typeConfig)
	return typeConfig
}

func CrdForAPIResource(apiResource metav1.APIResource) *apiextv1b1.CustomResourceDefinition {
	scope := apiextv1b1.ClusterScoped
	if apiResource.Namespaced {
		scope = apiextv1b1.NamespaceScoped
	}
	return &apiextv1b1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: groupQualifiedName(apiResource),
		},
		Spec: apiextv1b1.CustomResourceDefinitionSpec{
			Group:   apiResource.Group,
			Version: apiResource.Version,
			Scope:   scope,
			Names: apiextv1b1.CustomResourceDefinitionNames{
				Plural:   apiResource.Name,
				Singular: apiResource.SingularName,
				Kind:     apiResource.Kind,
			},
		},
	}
}

func groupQualifiedName(apiResource metav1.APIResource) string {
	return fmt.Sprintf("%s.%s", apiResource.Name, apiResource.Group)

}
