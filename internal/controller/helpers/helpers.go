package helpers

import (
	"context"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"

	"github.com/s0ders/k8s-reloader/internal/controller/annotation"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func getPodTemplate(obj client.Object) (*corev1.PodTemplateSpec, error) {
	switch o := obj.(type) {
	case *appsv1.Deployment:
		return &o.Spec.Template, nil
	case *appsv1.ReplicaSet:
		return &o.Spec.Template, nil
	case *appsv1.DaemonSet:
		return &o.Spec.Template, nil
	case *appsv1.StatefulSet:
		return &o.Spec.Template, nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", obj)
	}
}

func ReferencesConfigMap(obj client.Object, configMapName string) bool {
	template, err := getPodTemplate(obj)
	if err != nil {
		return false
	}

	// Check volumes
	for _, vol := range template.Spec.Volumes {
		if vol.ConfigMap != nil && vol.ConfigMap.Name == configMapName {
			return true
		}
	}
	// Check envFrom on each container
	for _, container := range template.Spec.Containers {
		for _, envFrom := range container.EnvFrom {
			if envFrom.ConfigMapRef != nil && envFrom.ConfigMapRef.Name == configMapName {
				return true
			}
		}
		// Check individual env vars sourced from configmap
		for _, env := range container.Env {
			if env.ValueFrom != nil &&
				env.ValueFrom.ConfigMapKeyRef != nil &&
				env.ValueFrom.ConfigMapKeyRef.Name == configMapName {
				return true
			}
		}
	}
	return false
}

func ReferencedConfigMaps(ctx context.Context, obj client.Object, k8sClient client.Client) ([]corev1.ConfigMap, error) {
	template, err := getPodTemplate(obj)
	if err != nil {
		return nil, fmt.Errorf("error getting pod template: %v", err)
	}

	configMapNames := make(map[string]struct{})

	// Check volumes
	for _, vol := range template.Spec.Volumes {
		if vol.ConfigMap != nil {
			if _, ok := configMapNames[vol.ConfigMap.Name]; !ok {
				configMapNames[vol.ConfigMap.Name] = struct{}{}
			}
		}
	}
	// Check envFrom on each container
	for _, container := range template.Spec.Containers {
		for _, envFrom := range container.EnvFrom {
			if envFrom.ConfigMapRef != nil {
				if _, ok := configMapNames[envFrom.ConfigMapRef.Name]; !ok {
					configMapNames[envFrom.ConfigMapRef.Name] = struct{}{}
				}
			}
		}
		// Check individual env vars sourced from configmap
		for _, env := range container.Env {
			if env.ValueFrom != nil && env.ValueFrom.ConfigMapKeyRef != nil {
				if _, ok := configMapNames[env.ValueFrom.ConfigMapKeyRef.Name]; !ok {
					configMapNames[env.ValueFrom.ConfigMapKeyRef.Name] = struct{}{}
				}
			}
		}
	}

	configMaps := make([]corev1.ConfigMap, 0, len(configMapNames))

	for configMapName := range configMapNames {
		var cm corev1.ConfigMap

		err = k8sClient.Get(ctx, types.NamespacedName{Name: configMapName, Namespace: obj.GetNamespace()}, &cm)
		if err != nil {
			continue
		}

		configMaps = append(configMaps, cm)
	}

	return configMaps, nil
}

func ReferencedSecrets(ctx context.Context, obj client.Object, c client.Client) ([]corev1.Secret, error) {
	template, err := getPodTemplate(obj)
	if err != nil {
		return nil, fmt.Errorf("error getting pod template: %v", err)
	}

	secretNames := make(map[string]struct{})

	// Check volumes
	for _, vol := range template.Spec.Volumes {
		if vol.Secret != nil {
			if _, ok := secretNames[vol.Secret.SecretName]; !ok {
				secretNames[vol.Secret.SecretName] = struct{}{}
			}
		}
	}
	// Check envFrom on each container
	for _, container := range template.Spec.Containers {
		for _, envFrom := range container.EnvFrom {
			if envFrom.SecretRef != nil {
				if _, ok := secretNames[envFrom.SecretRef.Name]; !ok {
					secretNames[envFrom.SecretRef.Name] = struct{}{}
				}
			}
		}
		// Check individual env vars sourced from configmap
		for _, env := range container.Env {
			if env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil {
				if _, ok := secretNames[env.ValueFrom.SecretKeyRef.Name]; !ok {
					secretNames[env.ValueFrom.SecretKeyRef.Name] = struct{}{}
				}
			}
		}
	}

	secrets := make([]corev1.Secret, 0, len(secretNames))

	for secretName := range secretNames {
		var secret corev1.Secret

		err = c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: obj.GetNamespace()}, &secret)
		if err != nil {
			continue
		}

		secrets = append(secrets, secret)
	}

	return secrets, nil
}

func ReferencesSecret(obj client.Object, secretName string) bool {
	template, err := getPodTemplate(obj)
	if err != nil {
		return false
	}

	// Check volumes
	for _, vol := range template.Spec.Volumes {
		if vol.Secret != nil && vol.Secret.SecretName == secretName {
			return true
		}
	}
	// Check envFrom on each container
	for _, container := range template.Spec.Containers {
		for _, envFrom := range container.EnvFrom {
			if envFrom.SecretRef != nil && envFrom.SecretRef.Name == secretName {
				return true
			}
		}
		// Check individual env vars sourced from configmap
		for _, env := range container.Env {
			if env.ValueFrom != nil &&
				env.ValueFrom.SecretKeyRef != nil &&
				env.ValueFrom.SecretKeyRef.Name == secretName {
				return true
			}
		}
	}
	return false
}

func ReloadingEnabled(obj client.Object) (bool, error) {
	annotations := obj.GetAnnotations()

	if _, found := annotations[annotation.ReloaderEnabledAnnotation]; !found {
		return false, nil
	}

	b, err := strconv.ParseBool(annotations[annotation.ReloaderEnabledAnnotation])
	if err != nil {
		return false, fmt.Errorf("failed to parse reloader enabled annotation %q: %w", annotation.ReloaderEnabledAnnotation, err)
	}

	return b, nil
}

func ShouldReloadOnConfigMapChange(obj client.Object, configMapName string) (bool, error) {
	genericReloading, err := ReloadingEnabled(obj)
	if err != nil {
		return false, err
	}

	// Object reloads on any changes.
	if genericReloading {
		return true, nil
	}

	annotations := obj.GetAnnotations()

	// Object does not have global reloading enabled nor ConfigMap specific reloading.
	if _, found := annotations[annotation.ReloaderConfigMapEnabledAnnotation]; !found {
		return false, nil
	}

	// Check if the ConfigMaps the object wants to reload on contain the one that changed.
	configMapNames := strings.Split(annotations[annotation.ReloaderConfigMapEnabledAnnotation], ",")
	for _, name := range configMapNames {
		if name == configMapName {
			return true, nil
		}
	}

	// The ConfigMap that changed is not part of the ConfigMap list the object wants to reload on change.
	return false, nil
}

func ShouldReloadOnSecretChange(obj client.Object, secretName string) (bool, error) {
	genericReloading, err := ReloadingEnabled(obj)
	if err != nil {
		return false, err
	}

	// Object reloads on any changes.
	if genericReloading {
		return true, nil
	}

	annotations := obj.GetAnnotations()

	// Object does not have global reloading enabled nor Secret specific reloading.
	if _, found := annotations[annotation.ReloaderSecretEnabledAnnotation]; !found {
		return false, nil
	}

	// Check if the Secrets the object wants to reload on contain the one that changed.
	secretNames := strings.Split(annotations[annotation.ReloaderSecretEnabledAnnotation], ",")
	for _, name := range secretNames {
		if name == secretName {
			return true, nil
		}
	}

	// The Secret that changed is not part of the Secret list the object wants to reload on change.
	return false, nil
}

func IgnoreReloading(obj client.Object) (bool, error) {
	annotations := obj.GetAnnotations()

	if _, found := annotations[annotation.ReloaderIgnoreAnnotation]; !found {
		return false, nil
	}

	b, err := strconv.ParseBool(annotations[annotation.ReloaderIgnoreAnnotation])
	if err != nil {
		return false, fmt.Errorf("failed to parse reloader ignore annotation: %w", err)
	}

	return b, nil
}

func HashConfigMapData(data map[string]string) string {
	keys := make([]string, 0, len(data))

	for k := range data {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	hash := fnv.New64a()

	for _, k := range keys {
		_, _ = hash.Write([]byte(data[k]))
	}

	return fmt.Sprintf("%x", hash.Sum64())
}

func HashSecretData(data map[string][]byte) string {
	keys := make([]string, 0, len(data))

	for k := range data {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	hash := fnv.New64a()

	for _, k := range keys {
		_, _ = hash.Write(data[k])
	}

	return fmt.Sprintf("%x", hash.Sum64())
}
