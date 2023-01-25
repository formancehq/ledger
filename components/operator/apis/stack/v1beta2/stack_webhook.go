/*
Copyright 2022.

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

package v1beta2

import (
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

const (
	DefaultVersions = "default"
)

var _ webhook.Defaulter = &Stack{}

// log is for logging in this package.
//
//nolint:unused
var stacklog = logf.Log.WithName("stack-resource")

//+kubebuilder:webhook:path=/mutate-stack-formance-com-v1beta2-stack,mutating=true,failurePolicy=fail,sideEffects=None,groups=stack.formance.com,resources=stacks,verbs=create;update,versions=v1beta2,name=mstacks.kb.io,admissionReviewVersions=v1

func (r *Stack) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

func (r *Stack) Default() {
	if r.Spec.Versions == "" {
		r.Spec.Versions = DefaultVersions
	}
}
