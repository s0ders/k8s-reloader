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

// ReplicaSetReloaderReconciler reconciles a ReplicaSetReloader object
type ReplicaSetReloaderReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *ReplicaSetReloaderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ReplicaSetReloaderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.ReplicaSet{}).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.findReplicaSetsForConfigMap),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.findReplicaSetsForSecret),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Named("replicaSetReloader").
		Complete(r)
}

func (r *ReplicaSetReloaderReconciler) findReplicaSetsForConfigMap(ctx context.Context, obj client.Object) []reconcile.Request {
	var cm corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKeyFromObject(obj), &cm); err != nil {
		return nil
	}

	var replicaSetList appsv1.ReplicaSetList
	if err := r.List(ctx, &replicaSetList, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	var result []reconcile.Request

	for _, replicaSet := range replicaSetList.Items {
		if replicaSetReferencesConfigMap(replicaSet, cm.GetName()) {
			result = append(result, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      replicaSet.Name,
					Namespace: replicaSet.Namespace,
				},
			})
		}
	}

	return result
}

func replicaSetReferencesConfigMap(replicaSet appsv1.ReplicaSet, configMapName string) bool {
	// Check volumes
	for _, vol := range replicaSet.Spec.Template.Spec.Volumes {
		if vol.ConfigMap != nil && vol.ConfigMap.Name == configMapName {
			return true
		}
	}
	// Check envFrom on each container
	for _, container := range replicaSet.Spec.Template.Spec.Containers {
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

func (r *ReplicaSetReloaderReconciler) findReplicaSetsForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var secret corev1.Secret
	if err := r.Get(ctx, client.ObjectKeyFromObject(obj), &secret); err != nil {
		return nil
	}

	var replicaSetList appsv1.ReplicaSetList
	if err := r.List(ctx, &replicaSetList, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	var result []reconcile.Request

	for _, replicaSet := range replicaSetList.Items {
		if replicaSetReferencesSecret(replicaSet, secret.GetName()) {
			result = append(result, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      replicaSet.Name,
					Namespace: replicaSet.Namespace,
				},
			})
		}
	}

	return result
}

func replicaSetReferencesSecret(replicaSet appsv1.ReplicaSet, secretName string) bool {
	// Check volumes
	for _, vol := range replicaSet.Spec.Template.Spec.Volumes {
		if vol.Secret != nil && vol.Secret.SecretName == secretName {
			return true
		}
	}
	// Check envFrom on each container
	for _, container := range replicaSet.Spec.Template.Spec.Containers {
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
