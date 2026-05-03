package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/s0ders/k8s-reloader/internal/controller/annotation"
	"github.com/s0ders/k8s-reloader/internal/controller/helpers"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func FindWorkloadsForConfigMap[T client.Object](
	ctx context.Context,
	c client.Client,
	obj client.Object,
	listWorkloads func(ctx context.Context, namespace string) ([]T, error),
) []reconcile.Request {
	log := logf.FromContext(ctx)

	var cm corev1.ConfigMap
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), &cm); err != nil {
		return nil
	}

	// Check if ConfigMap should be ignored first.
	ignore, err := helpers.IgnoreReloading(&cm)
	if err != nil {
		log.Error(
			err,
			"failed to parse the annotation on configmap, should be a boolean",
			"configmap", obj.GetName(),
			"namespace", obj.GetNamespace(),
			"annotation", obj.GetAnnotations()[annotation.ReloaderIgnoreAnnotation],
		)
		return nil
	}

	if ignore {
		return nil
	}

	var requests []reconcile.Request

	items, err := listWorkloads(ctx, obj.GetNamespace())
	if err != nil {
		log.Error(
			err, "listing workloads for configmap",
			"configmap", obj.GetName(),
			"namespace", obj.GetNamespace(),
		)
		return nil
	}

	for _, item := range items {
		shouldReload, err := helpers.ShouldReloadOnConfigMapChange(item, cm.GetName())
		if err != nil {
			log.Error(
				err, "failed to parse reloading annotations",
				"deployment", item.GetName(),
				"namespace", item.GetNamespace(),
			)
			continue
		}

		// For a deployment to be reconciled it must reference the ConfigMap and have reloading enabled either globally
		// or for that specific ConfigMap.
		if helpers.ReferencesConfigMap(item, obj.GetName()) && shouldReload {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: item.GetNamespace(),
					Name:      item.GetName(),
				},
			})
		}
	}

	return requests
}

func FindWorkloadsForSecret[T client.Object](
	ctx context.Context,
	c client.Client,
	obj client.Object,
	listWorkloads func(ctx context.Context, namespace string) ([]T, error),
) []reconcile.Request {
	log := logf.FromContext(ctx)

	var secret corev1.Secret
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), &secret); err != nil {
		return nil
	}

	// Check if ConfigMap should be ignored first.
	ignore, err := helpers.IgnoreReloading(&secret)
	if err != nil {
		log.Error(
			err,
			"failed to parse the annotation on secret, should be a boolean",
			"configmap", obj.GetName(),
			"namespace", obj.GetNamespace(),
			"annotation", obj.GetAnnotations()[annotation.ReloaderIgnoreAnnotation],
		)
		return nil
	}

	if ignore {
		return nil
	}

	var requests []reconcile.Request

	items, err := listWorkloads(ctx, obj.GetNamespace())
	if err != nil {
		log.Error(
			err, "listing workloads for secret",
			"secret", obj.GetName(),
			"namespace", obj.GetNamespace(),
		)
		return nil
	}

	for _, item := range items {
		shouldReload, err := helpers.ShouldReloadOnSecretChange(item, secret.GetName())
		if err != nil {
			log.Error(
				err, "failed to parse reloading annotations",
				"deployment", item.GetName(),
				"namespace", item.GetNamespace(),
			)
			continue
		}

		// For a deployment to be reconciled it must reference the ConfigMap and have reloading enabled either globally
		// or for that specific ConfigMap.
		if helpers.ReferencesSecret(item, obj.GetName()) && shouldReload {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: item.GetNamespace(),
					Name:      item.GetName(),
				},
			})
		}
	}

	return requests
}

func GenericReconcile[T client.Object](
	ctx context.Context,
	req ctrl.Request,
	c client.Client,
	getObject func(req ctrl.Request) (T, error),
	getPodTemplateAnnotations func(obj T) map[string]string,
	setPodTemplateAnnotations func(obj T, annotations map[string]string),
) (ctrl.Result, error) {
	obj, err := getObject(req)
	if err != nil {
		return ctrl.Result{}, err
	}

	configMaps, err := helpers.ReferencedConfigMaps(ctx, obj, c)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("referenced configmap: %w", err)
	}

	secrets, err := helpers.ReferencedSecrets(ctx, obj, c)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("referenced secrets: %w", err)
	}

	configMapHashes := make(map[string]string, len(configMaps))
	for _, configMap := range configMaps {
		configMapHashes[configMap.Name] = helpers.HashConfigMapData(configMap.Data)
	}

	secretHashes := make(map[string]string, len(secrets))
	for _, secret := range secrets {
		secretHashes[secret.Name] = helpers.HashSecretData(secret.Data)
	}

	triggerRollout := false

	annotations := obj.GetAnnotations()

	for configMapName, configMapHash := range configMapHashes {
		hashAnnotation := annotation.ConfigMapHashAnnotation(configMapName)
		currentHash, found := annotations[hashAnnotation]

		if !found || currentHash != configMapHash {
			annotations[hashAnnotation] = configMapHash
			obj.SetAnnotations(annotations)
			triggerRollout = true
		}
	}

	for secretName, secretHash := range secretHashes {
		hashAnnotation := annotation.SecretHashAnnotation(secretName)
		currentHash, found := annotations[hashAnnotation]

		if !found || currentHash != secretHash {
			annotations[hashAnnotation] = secretHash
			obj.SetAnnotations(annotations)
			triggerRollout = true
		}
	}

	if triggerRollout {
		podAnnotations := getPodTemplateAnnotations(obj)
		if podAnnotations == nil {
			podAnnotations = make(map[string]string)
		}

		podAnnotations[annotation.ReloaderTimestampAnnotation] = time.Now().UTC().Format(time.RFC3339)
		setPodTemplateAnnotations(obj, podAnnotations)
	}

	if err := c.Update(ctx, obj); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update deployment: %w", err)
	}

	return ctrl.Result{}, nil
}
