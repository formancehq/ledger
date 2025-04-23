package provisionner

import (
	"bytes"
	"context"
	"errors"
	"github.com/formancehq/go-libs/v3/pointer"
	"gopkg.in/yaml.v3"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"os"
)

type Store interface {
	Read(ctx context.Context) (*State, error)
	Update(ctx context.Context, state State) error
}

type FileStateStore struct {
	path string
}

func (f *FileStateStore) Read(_ context.Context) (*State, error) {
	file, err := os.Open(f.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return pointer.For(newState()), nil
		}
		return nil, err
	}

	state := State{}
	if err := yaml.NewDecoder(file).Decode(&state); err != nil {
		return nil, err
	}

	return &state, nil
}

func (f *FileStateStore) Update(_ context.Context, state State) error {
	file, err := os.Create(f.path)
	if err != nil {
		return err
	}

	return yaml.NewEncoder(file).Encode(state)
}

var _ Store = (*FileStateStore)(nil)

func NewFileStore(path string) *FileStateStore {
	return &FileStateStore{path: path}
}

type K8sConfigMapStore struct {
	client        *kubernetes.Clientset
	namespace     string
	configMapName string
}

func (k *K8sConfigMapStore) Read(ctx context.Context) (*State, error) {
	configMap, err := k.client.CoreV1().
		ConfigMaps(k.namespace).
		Get(ctx, k.configMapName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return pointer.For(newState()), nil
		}
		return nil, err
	}

	rawState := configMap.Data["state.yaml"]
	ret := &State{}
	if err := yaml.NewDecoder(bytes.NewBufferString(rawState)).Decode(ret); err != nil {
		return nil, err
	}

	return ret, nil
}

func (k *K8sConfigMapStore) Update(ctx context.Context, state State) error {

	marshalledState, err := yaml.Marshal(state)
	if err != nil {
		return err
	}

	configMap, err := k.client.CoreV1().
		ConfigMaps(k.namespace).
		Get(ctx, k.configMapName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, err := k.client.CoreV1().
				ConfigMaps(k.namespace).
				Create(ctx, &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: k.namespace,
						Name:      k.configMapName,
					},
					Data: map[string]string{
						"state.yaml": string(marshalledState),
					},
				}, metav1.CreateOptions{})
			return err
		}
		return err
	}

	configMap.Data["state.yaml"] = string(marshalledState)

	_, err = k.client.CoreV1().
		ConfigMaps(k.namespace).
		Update(ctx, configMap, metav1.UpdateOptions{})

	return err
}

var _ Store = (*K8sConfigMapStore)(nil)

func NewK8SConfigMapStore(namespace, configMapName string) (*K8sConfigMapStore, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &K8sConfigMapStore{
		client:        client,
		namespace:     namespace,
		configMapName: configMapName,
	}, nil
}
