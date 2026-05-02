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

	unixTimestamp := time.Now().Unix()

	deployment.Spec.Template.Annotations[reloaderTimestampAnnotation] = strconv.Itoa(int(unixTimestamp))

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

	// Check if ConfigMap should be ignored first
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
	if err := r.List(ctx, &deployments, client.InNamespace(obj.GetNamespace())); err != nil {
		return []reconcile.Request{}
	}

	// Filter deployment to see if they reference the ConfigMap and if they wish to reload on changes
	var requests []reconcile.Request

	for _, deployment := range deployments.Items {
		shouldReload, err := reloadingEnabled(deployment.ObjectMeta)
		if err != nil {
			log.Error(
				err,
				"failed to parse the annotation on deployment, should be a boolean",
				"deployment", deployment.GetName(),
				"namespace", deployment.GetNamespace(),
				"annotation", deployment.Annotations[reloaderEnabledAnnotation],
			)
			continue
		}

		shouldReloadOnConfigMap, err := configMapReloadingEnabled(deployment.ObjectMeta)
		if err != nil {
			log.Error(
				err,
				"failed to parse the annotations on deployment, should be a boolean",
				"deployment", deployment.GetName(),
				"namespace", deployment.GetNamespace(),
				"annotation", deployment.Annotations[reloaderConfigMapEnabledAnnotation],
			)
			continue
		}

		if !shouldReload && !shouldReloadOnConfigMap {
			continue
		}

		if deploymentReferencesConfigMap(deployment, obj.GetName()) {
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

func deploymentReferencesConfigMap(deployment appsv1.Deployment, configMapName string) bool {
	// Check volumes
	for _, vol := range deployment.Spec.Template.Spec.Volumes {
		if vol.ConfigMap != nil && vol.ConfigMap.Name == configMapName {
			return true
		}
	}
	// Check envFrom on each container
	for _, container := range deployment.Spec.Template.Spec.Containers {
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

func (r *DeploymentReloaderReconciler) findDeploymentsForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logf.FromContext(ctx)

	var secret corev1.Secret
	if err := r.Get(ctx, client.ObjectKeyFromObject(obj), &secret); err != nil {
		return nil
	}

	// Check if ConfigMap should be ignored first
	ignore, err := ignoreReloading(secret.ObjectMeta)
	if err != nil {
		log.Error(
			err,
			"failed to parse the annotation on secret, should be a boolean",
			"configmap", obj.GetName(),
			"namespace", obj.GetNamespace(),
			"annotation", secret.Annotations[reloaderIgnoreAnnotation],
		)
		return nil
	}

	if ignore {
		return nil
	}

	var deployments appsv1.DeploymentList
	if err := r.List(ctx, &deployments, client.InNamespace(obj.GetNamespace())); err != nil {
		return []reconcile.Request{}
	}

	var requests []reconcile.Request

	for _, deployment := range deployments.Items {
		shouldReload, err := reloadingEnabled(deployment.ObjectMeta)
		if err != nil {
			log.Error(
				err,
				"failed to parse the annotation on deployment, should be a boolean",
				"deployment", deployment.GetName(),
				"namespace", deployment.GetNamespace(),
				"annotation", deployment.Annotations[reloaderEnabledAnnotation],
			)
			continue
		}

		shouldReloadOnConfigMap, err := secretReloadingEnabled(deployment.ObjectMeta)
		if err != nil {
			log.Error(
				err,
				"failed to parse the annotations on deployment, should be a boolean",
				"deployment", deployment.GetName(),
				"namespace", deployment.GetNamespace(),
				"annotation", deployment.Annotations[reloaderSecretEnabledAnnotation],
			)
			continue
		}

		if !shouldReload && !shouldReloadOnConfigMap {
			continue
		}

		if deploymentReferencesSecret(deployment, obj.GetName()) {
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

func deploymentReferencesSecret(deployment appsv1.Deployment, secretName string) bool {

	// Check volumes
	for _, vol := range deployment.Spec.Template.Spec.Volumes {
		if vol.Secret != nil && vol.Secret.SecretName == secretName {
			return true
		}
	}
	// Check envFrom on each container
	for _, container := range deployment.Spec.Template.Spec.Containers {
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
