package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func vct(name, size string) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(size),
				},
			},
		},
	}
}

func TestVolumeClaimTemplateSizeChanges(t *testing.T) {
	t.Parallel()

	t.Run("no changes", func(t *testing.T) {
		grown, shrunk := volumeClaimTemplateSizeChanges(
			[]corev1.PersistentVolumeClaim{vct("wal", "5Gi"), vct("data", "10Gi")},
			[]corev1.PersistentVolumeClaim{vct("wal", "5Gi"), vct("data", "10Gi")},
		)
		assert.Empty(t, grown)
		assert.Empty(t, shrunk)
	})

	t.Run("grown template is reported with its target size", func(t *testing.T) {
		grown, shrunk := volumeClaimTemplateSizeChanges(
			[]corev1.PersistentVolumeClaim{vct("wal", "5Gi"), vct("data", "10Gi")},
			[]corev1.PersistentVolumeClaim{vct("wal", "5Gi"), vct("data", "250Gi")},
		)
		assert.Empty(t, shrunk)
		if assert.Contains(t, grown, "data") {
			want := resource.MustParse("250Gi")
			got := grown["data"]
			assert.Zero(t, got.Cmp(want))
		}
	})

	t.Run("shrunk template is reported by name and not grown", func(t *testing.T) {
		grown, shrunk := volumeClaimTemplateSizeChanges(
			[]corev1.PersistentVolumeClaim{vct("wal", "5Gi"), vct("data", "10Gi")},
			[]corev1.PersistentVolumeClaim{vct("wal", "1Gi"), vct("data", "10Gi")},
		)
		assert.Empty(t, grown)
		assert.Equal(t, []string{"wal"}, shrunk)
	})

	t.Run("mixed grow and shrink are reported independently", func(t *testing.T) {
		grown, shrunk := volumeClaimTemplateSizeChanges(
			[]corev1.PersistentVolumeClaim{vct("wal", "5Gi"), vct("data", "10Gi")},
			[]corev1.PersistentVolumeClaim{vct("wal", "1Gi"), vct("data", "250Gi")},
		)
		assert.Contains(t, grown, "data")
		assert.Equal(t, []string{"wal"}, shrunk)
	})

	t.Run("templates present on only one side are ignored", func(t *testing.T) {
		grown, shrunk := volumeClaimTemplateSizeChanges(
			[]corev1.PersistentVolumeClaim{vct("wal", "5Gi")},
			[]corev1.PersistentVolumeClaim{vct("data", "250Gi")},
		)
		assert.Empty(t, grown)
		assert.Empty(t, shrunk)
	})
}
