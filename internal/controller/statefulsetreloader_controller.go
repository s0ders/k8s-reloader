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

// StatefulSetReloaderReconciler reconciles a StatefulSetReloader object
type StatefulSetReloaderReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *StatefulSetReloaderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return GenericReconcile(
		ctx,
		req,
		r.Client,
		func(req ctrl.Request) (*appsv1.StatefulSet, error) {
			var statefulSet appsv1.StatefulSet
			if err := r.Get(ctx, req.NamespacedName, &statefulSet); err != nil {
				return nil, err
			}
			return &statefulSet, nil
		},
		func(obj *appsv1.StatefulSet) map[string]string {
			return obj.Spec.Template.Annotations
		},
		func(obj *appsv1.StatefulSet, annotations map[string]string) {
			obj.Spec.Template.Annotations = annotations
		},
	)
}

// SetupWithManager sets up the controller with the Manager.
func (r *StatefulSetReloaderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.StatefulSet{}).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.findStatefulSetsForConfigMap),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.findStatefulSetsForSecret),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Named("statefulSetReloader").
		Complete(r)
}

func (r *StatefulSetReloaderReconciler) findStatefulSetsForConfigMap(ctx context.Context, obj client.Object) []reconcile.Request {
	return FindWorkloadsForConfigMap(ctx, r.Client, obj, func(ctx context.Context, namespace string) ([]*appsv1.StatefulSet, error) {
		var list appsv1.StatefulSetList
		err := r.List(ctx, &list, client.InNamespace(namespace))

		pointerList := make([]*appsv1.StatefulSet, len(list.Items))

		for i, item := range list.Items {
			pointerList[i] = &item
		}

		return pointerList, err
	})
}

func (r *StatefulSetReloaderReconciler) findStatefulSetsForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	return FindWorkloadsForSecret(ctx, r.Client, obj, func(ctx context.Context, namespace string) ([]*appsv1.StatefulSet, error) {
		var list appsv1.StatefulSetList
		err := r.List(ctx, &list, client.InNamespace(namespace))

		pointerList := make([]*appsv1.StatefulSet, len(list.Items))

		for i, item := range list.Items {
			pointerList[i] = &item
		}

		return pointerList, err
	})
}
