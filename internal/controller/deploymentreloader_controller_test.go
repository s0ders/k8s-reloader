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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/s0ders/k8s-reloader/internal/controller/annotation"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

var _ = Describe("DeploymentReloader Controller", func() {
	Context("When reconciling a resource", func() {

		const (
			NamespaceName  = "dummy-ns"
			ConfigMapName  = "dummy-cm"
			DeploymentName = "dummy-deploy"

			ConfigMapKeyName = "dummy-key"
			AppName          = "dummy-app"
			VolumeName       = "dummy-volume"
		)

		ctx := context.Background()

		namespace := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:      NamespaceName,
				Namespace: NamespaceName,
			},
		}

		configMapNamespacedName := types.NamespacedName{
			Name:      ConfigMapName,
			Namespace: NamespaceName,
		}

		deploymentNamespacedName := types.NamespacedName{
			Name:      DeploymentName,
			Namespace: NamespaceName,
		}

		configMap := &corev1.ConfigMap{}
		deployment := &appsv1.Deployment{}

		SetDefaultEventuallyTimeout(2 * time.Minute)
		SetDefaultEventuallyPollingInterval(time.Second)

		BeforeEach(func() {
			By("Creating the Namespace to perform the tests")
			err := k8sClient.Get(ctx, types.NamespacedName{Name: NamespaceName}, &corev1.Namespace{})
			if err != nil && errors.IsNotFound(err) {
				err = k8sClient.Create(ctx, namespace)
				Expect(err).NotTo(HaveOccurred())
			}

			By("creating the test ConfigMap")
			configMap = &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, configMapNamespacedName, configMap)
			if err != nil && errors.IsNotFound(err) {
				configMap = &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      ConfigMapName,
						Namespace: namespace.Name,
					},
					Data: map[string]string{
						ConfigMapKeyName: "lorem ipsum",
					},
				}
				err = k8sClient.Create(ctx, configMap)
				Expect(err).NotTo(HaveOccurred())
			}

			By("creating the test Deployment")
			deployment = &appsv1.Deployment{}
			err = k8sClient.Get(ctx, deploymentNamespacedName, deployment)
			if err != nil && errors.IsNotFound(err) {
				deployment = &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      DeploymentName,
						Namespace: namespace.Name,
						Annotations: map[string]string{
							annotation.ReloaderEnabledAnnotation: "true",
						},
					},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"app": AppName,
							},
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{
									"app": AppName,
								},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "nginx-container",
										Image: "nginx:latest",
										VolumeMounts: []corev1.VolumeMount{
											{
												Name:      VolumeName,
												MountPath: "/config",
												ReadOnly:  true,
											},
										},
									},
								},
								Volumes: []corev1.Volume{
									{
										Name: VolumeName,
										VolumeSource: corev1.VolumeSource{
											ConfigMap: &corev1.ConfigMapVolumeSource{
												LocalObjectReference: corev1.LocalObjectReference{
													Name: ConfigMapName,
												},
											},
										},
									},
								},
							},
						},
					},
				}
				err = k8sClient.Create(ctx, deployment)
				Expect(err).NotTo(HaveOccurred())
			}
		})

		AfterEach(func() {
			By("removing the test ConfigMap")
			foundConfigMap := &corev1.ConfigMap{}
			err := k8sClient.Get(ctx, configMapNamespacedName, foundConfigMap)
			Expect(err).NotTo(HaveOccurred())

			By("removing the test Deployment")
			foundDeployment := &appsv1.Deployment{}
			err = k8sClient.Get(ctx, deploymentNamespacedName, foundDeployment)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Delete(context.TODO(), foundConfigMap)).To(Succeed())
			}).Should(Succeed())

			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Delete(context.TODO(), foundDeployment)).To(Succeed())
			}).Should(Succeed())

			// TODO(user): Attention if you improve this code by adding other context test you MUST
			// be aware of the current delete namespace limitations.
			// More info: https://book.kubebuilder.io/reference/envtest.html#testing-considerations
			By("Deleting the Namespace to perform the tests")
			_ = k8sClient.Delete(ctx, namespace)
		})

		It("should successfully rollout the Deployment's Pods when the ConfigMap data is updated", func() {

			By("checking that the annotation is not present yet")
			deployment = &appsv1.Deployment{}
			err := k8sClient.Get(ctx, deploymentNamespacedName, deployment)
			Expect(err).NotTo(HaveOccurred())
			_, found := deployment.Spec.Template.Annotations[annotation.ReloaderTimestampAnnotation]
			Expect(found).To(BeFalse())

			By("changing the data inside the ConfigMap")
			configMap = &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, configMapNamespacedName, configMap)
			Expect(err).NotTo(HaveOccurred())

			configMap.Data[ConfigMapKeyName] = "new-data"

			err = k8sClient.Update(ctx, configMap)
			Expect(err).NotTo(HaveOccurred())

			By("checking that the annotation is present")
			Eventually(func(g Gomega) {
				deployment = &appsv1.Deployment{}
				err = k8sClient.Get(ctx, deploymentNamespacedName, deployment)
				g.Expect(err).NotTo(HaveOccurred())

				_, found := deployment.Spec.Template.Annotations[annotation.ReloaderTimestampAnnotation]
				g.Expect(found).To(BeTrue())
			}).Should(Succeed())
		})
	})
})
