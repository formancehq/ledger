package main

import (
	"context"
	"log"
	"os"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/test/antithesis/internal"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	// get latest version
	latest_tag, err := os.ReadFile("/ledger_latest_tag")
	if err != nil {
		log.Fatal(err)
	}

	// build dynamic client
	config, err := clientcmd.BuildConfigFromFlags("", "")
	if err != nil {
		panic(err)
	}

	dyn, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	gvr := schema.GroupVersionResource {
		Group:    "formance.com",
		Version:  "v1beta1",
		Resource: "ledgers",
	}

	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "formance.com/v1beta1",
			"kind": "Ledger",
			"metadata": map[string]interface{}{
				"name": "stack0-ledger",
			},
			"spec": map[string]interface{}{
				"stack": "stack0",
				"version": string(latest_tag),
			},
		},
	}

	// update Ledger custom resource to next version
	res, err := dyn.Resource(gvr).Namespace("formance-systems").Update(context.Background(), obj, metav1.UpdateOptions{})

	assert.Sometimes(err == nil, "successfully", internal.Details{
		"ledger": res,
	})
	if err != nil {
		panic(err)
	}

	log.Println("placeholder command for anytime_version_upgrade")
}
