/*
Copyright 2026.

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

package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// DeploymentReloaderReconciler reconciles a DeploymentReloader object
type DeploymentReloaderReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// The filtering of deployments that should be reconciled is happens in the handler (checking if reloading is enabled,
// etc.), the reconcile assumes the deployment should be reloaded if it arrived this far in the process.
func (r *DeploymentReloaderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var deployment appsv1.Deployment
	if err := r.Get(ctx, req.NamespacedName, &deployment); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	configMaps, err := referencedConfigMaps(ctx, &deployment, r.Client)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("referenced configmap: %w", err)
	}

	secrets, err := referencedSecrets(ctx, &deployment, r.Client)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("referenced secrets: %w", err)
	}

	configMapHashes := make(map[string]string, len(configMaps))
	for _, configMap := range configMaps {
		configMapHashes[configMap.Name] = hashConfigMapData(configMap.Data)
	}

	secretHashes := make(map[string]string, len(secrets))
	for _, secret := range secrets {
		secretHashes[secret.Name] = hashSecretData(secret.Data)
	}

	triggerRollout := false

	for configMapName, configMapHash := range configMapHashes {
		hashAnnotation := configMapHashAnnotation(configMapName)

		if !metav1.HasAnnotation(deployment.ObjectMeta, hashAnnotation) {
			deployment.Annotations[hashAnnotation] = configMapHash
		} else if deployment.Annotations[hashAnnotation] != configMapHash {
			deployment.Annotations[hashAnnotation] = configMapHash
			triggerRollout = true
		}
	}

	for secretName, secretHash := range secretHashes {
		hashAnnotation := secretHashAnnotation(secretName)

		if !metav1.HasAnnotation(deployment.ObjectMeta, hashAnnotation) {
			deployment.Annotations[hashAnnotation] = secretHash
		} else if deployment.Annotations[hashAnnotation] != secretHash {
			deployment.Annotations[hashAnnotation] = secretHash
			triggerRollout = true
		}
	}

	if triggerRollout {
		unixTimestamp := time.Now().Unix()

		deployment.Spec.Template.Annotations[reloaderTimestampAnnotation] = strconv.Itoa(int(unixTimestamp))
	}

	if err := r.Update(ctx, &deployment); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update deployment: %w", err)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DeploymentReloaderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Deployment{}).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.findDeploymentsForConfigMap),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.findDeploymentsForSecret),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Named("deploymentReloader").
		Complete(r)
}

func (r *DeploymentReloaderReconciler) findDeploymentsForConfigMap(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logf.FromContext(ctx)

	var cm corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKeyFromObject(obj), &cm); err != nil {
		return nil
	}

	// Check if ConfigMap should be ignored first.
	ignore, err := ignoreReloading(cm.ObjectMeta)
	if err != nil {
		log.Error(
			err,
			"failed to parse the annotation on configmap, should be a boolean",
			"configmap", obj.GetName(),
			"namespace", obj.GetNamespace(),
			"annotation", cm.Annotations[reloaderIgnoreAnnotation],
		)
		return nil
	}

	if ignore {
		return nil
	}

	var deployments appsv1.DeploymentList
	if err = r.List(ctx, &deployments, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	var requests []reconcile.Request

	for _, deployment := range deployments.Items {
		shouldReload, err := shouldReloadOnConfigMapChange(deployment.ObjectMeta, cm.Name)
		if err != nil {
			log.Error(
				err, "failed to parse reloading annotations",
				"deployment", deployment.Name,
				"namespace", deployment.GetNamespace(),
			)
			continue
		}

		// For a deployment to be reconciled it must reference the ConfigMap and have reloading enabled either globally
		// or for that specific ConfigMap.
		if referencesConfigMap(&deployment, obj.GetName()) && shouldReload {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: deployment.Namespace,
					Name:      deployment.Name,
				},
			})
		}
	}

	return requests
}

func (r *DeploymentReloaderReconciler) findDeploymentsForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logf.FromContext(ctx)

	var secret corev1.Secret
	if err := r.Get(ctx, client.ObjectKeyFromObject(obj), &secret); err != nil {
		return nil
	}

	// Check if ConfigMap should be ignored first.
	ignore, err := ignoreReloading(secret.ObjectMeta)
	if err != nil {
		log.Error(
			err,
			"failed to parse the annotation on secret, should be a boolean",
			"secret", obj.GetName(),
			"namespace", obj.GetNamespace(),
			"annotation", secret.Annotations[reloaderIgnoreAnnotation],
		)
		return nil
	}

	if ignore {
		return nil
	}

	var deployments appsv1.DeploymentList
	if err = r.List(ctx, &deployments, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	var requests []reconcile.Request

	for _, deployment := range deployments.Items {
		shouldReload, err := shouldReloadOnSecretChange(deployment.ObjectMeta, secret.Name)
		if err != nil {
			log.Error(
				err, "failed to parse reloading annotations",
				"deployment", deployment.Name,
				"namespace", deployment.GetNamespace(),
			)
			continue
		}

		// For a deployment to be reconciled it must reference the ConfigMap and have reloading enabled either globally
		// or for that specific ConfigMap.
		if referencesSecret(&deployment, obj.GetName()) && shouldReload {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: deployment.Namespace,
					Name:      deployment.Name,
				},
			})
		}
	}

	return requests
}
