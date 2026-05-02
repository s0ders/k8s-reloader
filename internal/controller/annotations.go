package controller

import (
	"fmt"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	reloaderEnabledAnnotation          = "reloader.k8s.akira.sh/enabled"
	reloaderConfigMapEnabledAnnotation = "configmap.reloader.k8s.akira.sh/enabled"
	reloaderSecretEnabledAnnotation    = "secret.reloader.k8s.akira.sh/enabled"

	reloaderTimestampAnnotation = "reloader.k8s.akira.sh/reloadedAt"

	reloaderIgnoreAnnotation = "reloader.k8s.akira.sh/ignore"

	// Keys is a list of ConfigMap or Secret keys separated by semicolons such as "foo;bar;baz"
	// TODO: implement this check
	reloaderKeysAnnotation = "reloader.k8s.akira.sh/keys"
)

func reloadingEnabled(metadata metav1.ObjectMeta) (bool, error) {
	if !metav1.HasAnnotation(metadata, reloaderEnabledAnnotation) {
		return false, nil
	}

	b, err := strconv.ParseBool(metadata.Annotations[reloaderEnabledAnnotation])
	if err != nil {
		return false, fmt.Errorf("failed to parse reloader enabled annotation: %w", err)
	}

	return b, nil
}

func configMapReloadingEnabled(metadata metav1.ObjectMeta) (bool, error) {
	if !metav1.HasAnnotation(metadata, reloaderConfigMapEnabledAnnotation) {
		return false, nil
	}

	b, err := strconv.ParseBool(metadata.Annotations[reloaderConfigMapEnabledAnnotation])
	if err != nil {
		return false, fmt.Errorf("failed to parse reloader configmap enabled annotation: %w", err)
	}

	return b, nil
}

func secretReloadingEnabled(metadata metav1.ObjectMeta) (bool, error) {
	if !metav1.HasAnnotation(metadata, reloaderSecretEnabledAnnotation) {
		return false, nil
	}

	b, err := strconv.ParseBool(metadata.Annotations[reloaderSecretEnabledAnnotation])
	if err != nil {
		return false, fmt.Errorf("failed to parse reloader secret enabled annotation: %w", err)
	}

	return b, nil
}

func ignoreReloading(metadata metav1.ObjectMeta) (bool, error) {
	if !metav1.HasAnnotation(metadata, reloaderIgnoreAnnotation) {
		return false, nil
	}

	b, err := strconv.ParseBool(metadata.Annotations[reloaderIgnoreAnnotation])
	if err != nil {
		return false, fmt.Errorf("failed to parse reloader ignore annotation: %w", err)
	}

	return b, nil
}
