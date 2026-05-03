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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
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
	return GenericReconcile(
		ctx,
		req,
		r.Client,
		func(req ctrl.Request) (*appsv1.Deployment, error) {
			var deployment appsv1.Deployment
			if err := r.Get(ctx, req.NamespacedName, &deployment); err != nil {
				return nil, err
			}
			return &deployment, nil
		},
		func(obj *appsv1.Deployment) map[string]string {
			return obj.Spec.Template.Annotations
		},
		func(obj *appsv1.Deployment, annotations map[string]string) {
			obj.Spec.Template.Annotations = annotations
		},
	)
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
	return FindWorkloadsForConfigMap(ctx, r.Client, obj, func(ctx context.Context, namespace string) ([]*appsv1.Deployment, error) {
		var list appsv1.DeploymentList
		err := r.List(ctx, &list, client.InNamespace(namespace))

		pointerList := make([]*appsv1.Deployment, len(list.Items))

		for i, item := range list.Items {
			pointerList[i] = &item
		}

		return pointerList, err
	})
}

func (r *DeploymentReloaderReconciler) findDeploymentsForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	return FindWorkloadsForSecret(ctx, r.Client, obj, func(ctx context.Context, namespace string) ([]*appsv1.Deployment, error) {
		var list appsv1.DeploymentList
		err := r.List(ctx, &list, client.InNamespace(namespace))

		pointerList := make([]*appsv1.Deployment, len(list.Items))

		for i, item := range list.Items {
			pointerList[i] = &item
		}

		return pointerList, err
	})
}
