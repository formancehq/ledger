package controllerutils

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func copyDir(f fs.FS, root, path string, ret *map[string]string) error {
	dirEntries, err := fs.ReadDir(f, path)
	if err != nil {
		return err
	}
	for _, dirEntry := range dirEntries {
		dirEntryPath := filepath.Join(path, dirEntry.Name())
		if dirEntry.IsDir() {
			if err := copyDir(f, root, dirEntryPath, ret); err != nil {
				return err
			}
		} else {
			fileContent, err := fs.ReadFile(f, dirEntryPath)
			if err != nil {
				return err
			}
			sanitizedPath := strings.TrimPrefix(dirEntryPath, root)
			sanitizedPath = strings.TrimPrefix(sanitizedPath, "/")
			(*ret)[sanitizedPath] = string(fileContent)
		}
	}
	return nil
}

func CreateConfigMapFromDir(ctx context.Context, name types.NamespacedName, client client.Client, fs fs.FS,
	rootDir string, mutators ...ObjectMutator[*corev1.ConfigMap]) (controllerutil.OperationResult, error) {
	_, operationResult, err := CreateOrUpdate(ctx, client, name, append(mutators, func(configMap *corev1.ConfigMap) error {
		configMap.Data = map[string]string{}

		return copyDir(fs, rootDir, rootDir, &configMap.Data)
	})...)
	return operationResult, err
}
