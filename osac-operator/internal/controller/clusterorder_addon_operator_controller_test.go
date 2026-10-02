/*
Copyright 2025.

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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	osacv1alpha1 "github.com/osac-project/osac/osac-operator/api/v1alpha1"
	"github.com/osac-project/osac/osac-operator/pkg/provisioning"
)

type addOnOperatorProviderStub struct {
	triggeredOperators []string
	jobStatuses        map[string]provisioning.ProvisionStatus
	nextJobID          int
	adminKubeconfig    string
}

func newAddOnOperatorProviderStub() *addOnOperatorProviderStub {
	return &addOnOperatorProviderStub{jobStatuses: map[string]provisioning.ProvisionStatus{}}
}

func (p *addOnOperatorProviderStub) TriggerProvision(ctx context.Context, _ client.Object) (*provisioning.ProvisionResult, error) {
	operatorName := provisioning.AddOnOperatorNameFromContext(ctx)
	if operatorName == "" {
		return nil, fmt.Errorf("missing add-on operator name")
	}
	p.adminKubeconfig = provisioning.AdminKubeconfigFromContext(ctx)
	p.nextJobID++
	jobID := fmt.Sprintf("addon-job-%d", p.nextJobID)
	p.triggeredOperators = append(p.triggeredOperators, operatorName)
	p.jobStatuses[jobID] = provisioning.ProvisionStatus{
		JobID:   jobID,
		State:   osacv1alpha1.JobStatePending,
		Message: "queued",
	}
	return &provisioning.ProvisionResult{
		JobID:        jobID,
		InitialState: osacv1alpha1.JobStatePending,
		Message:      "queued",
	}, nil
}

func (p *addOnOperatorProviderStub) GetProvisionStatus(_ context.Context, _ client.Object, jobID string) (provisioning.ProvisionStatus, error) {
	return p.jobStatuses[jobID], nil
}

func (p *addOnOperatorProviderStub) TriggerDeprovision(_ context.Context, _ client.Object, _ []osacv1alpha1.JobStatus) (*provisioning.DeprovisionResult, error) {
	return nil, fmt.Errorf("unexpected deprovision dispatch")
}

func (p *addOnOperatorProviderStub) GetDeprovisionStatus(_ context.Context, _ client.Object, _ string) (provisioning.ProvisionStatus, error) {
	return provisioning.ProvisionStatus{}, fmt.Errorf("unexpected deprovision status poll")
}

func (p *addOnOperatorProviderStub) Name() string { return "add-on-test" }

func (p *addOnOperatorProviderStub) setJobStatus(jobID string, status provisioning.ProvisionStatus) {
	status.JobID = jobID
	p.jobStatuses[jobID] = status
}

var _ = Describe("AddOnOperatorReconciler", func() {
	const namespace = "default"
	ctx := context.Background()

	var (
		k8sClient  client.Client
		provider   *addOnOperatorProviderStub
		reconciler *AddOnOperatorReconciler
	)

	BeforeEach(func() {
		scheme := runtime.NewScheme()
		Expect(osacv1alpha1.AddToScheme(scheme)).To(Succeed())
		k8sClient = fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&osacv1alpha1.ClusterOrder{}).
			Build()
		provider = newAddOnOperatorProviderStub()
		reconciler = NewAddOnOperatorReconciler(k8sClient, k8sClient, namespace, provider, time.Minute)
		reconciler.getAdminKubeconfig = func(context.Context, *osacv1alpha1.ClusterOrder) ([]byte, error) {
			return []byte("test-kubeconfig"), nil
		}
	})

	newOrder := func(name string, phase osacv1alpha1.ClusterOrderPhaseType, operators ...string) *osacv1alpha1.ClusterOrder {
		return &osacv1alpha1.ClusterOrder{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: osacv1alpha1.ClusterOrderSpec{
				TemplateID:     "test.template",
				AddOnOperators: operators,
			},
			Status: osacv1alpha1.ClusterOrderStatus{Phase: phase},
		}
	}

	getOrder := func(name string) *osacv1alpha1.ClusterOrder {
		order := &osacv1alpha1.ClusterOrder{}
		ExpectWithOffset(1, k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, order)).To(Succeed())
		return order
	}

	It("does not dispatch until the ClusterOrder is Ready", func() {
		order := newOrder("not-ready", osacv1alpha1.ClusterOrderPhaseProgressing, "cert-manager")
		Expect(k8sClient.Create(ctx, order)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: order.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(provider.triggeredOperators).To(BeEmpty())

		order = getOrder(order.Name)
		order.Status.Phase = osacv1alpha1.ClusterOrderPhaseReady
		Expect(k8sClient.Status().Update(ctx, order)).To(Succeed())

		_, err = reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: order.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(provider.triggeredOperators).To(Equal([]string{"cert-manager"}))

		stored := getOrder(order.Name)
		Expect(stored.Status.AddOnOperatorJobs).To(HaveLen(1))
		Expect(stored.Status.AddOnOperatorJobs[0].Name).To(Equal("cert-manager"))
		Expect(stored.Status.AddOnOperatorJobs[0].State).To(Equal(osacv1alpha1.JobStatePending))
		Expect(stored.Status.Phase).To(Equal(osacv1alpha1.ClusterOrderPhaseReady))
	})

	It("skips unmanaged ClusterOrders", func() {
		order := newOrder("unmanaged", osacv1alpha1.ClusterOrderPhaseReady, "cert-manager")
		order.Annotations = map[string]string{osacManagementStateAnnotation: ManagementStateUnmanaged}
		Expect(k8sClient.Create(ctx, order)).To(Succeed())

		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: order.Name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(provider.triggeredOperators).To(BeEmpty())
	})

	It("marks successful operators installed without redispatching them", func() {
		order := newOrder("success", osacv1alpha1.ClusterOrderPhaseReady, "cert-manager")
		Expect(k8sClient.Create(ctx, order)).To(Succeed())
		request := ctrl.Request{NamespacedName: types.NamespacedName{Name: order.Name, Namespace: namespace}}

		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		jobID := getOrder(order.Name).Status.AddOnOperatorJobs[0].JobID
		provider.setJobStatus(jobID, provisioning.ProvisionStatus{State: osacv1alpha1.JobStateSucceeded, Message: "installed"})

		result, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeZero())
		stored := getOrder(order.Name)
		Expect(stored.Status.AddOnOperatorJobs[0].State).To(Equal(osacv1alpha1.JobStateSucceeded))
		Expect(stored.Status.AddOnOperatorJobs[0].Message).To(BeEmpty())
		condition := findAddOnOperatorCondition(stored)
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionTrue))

		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(provider.triggeredOperators).To(Equal([]string{"cert-manager"}))
	})

	It("retries only failed operators and keeps the main phase Ready", func() {
		order := newOrder("independent", osacv1alpha1.ClusterOrderPhaseReady, "cert-manager", "gpu-operator")
		order.Status.ProvisioningJobs = []osacv1alpha1.JobStatus{{
			JobID: "cluster-job", Type: osacv1alpha1.JobTypeProvision, State: osacv1alpha1.JobStateSucceeded,
			Timestamp: metav1.Now(),
		}}
		Expect(k8sClient.Create(ctx, order)).To(Succeed())
		request := ctrl.Request{NamespacedName: types.NamespacedName{Name: order.Name, Namespace: namespace}}

		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		stored := getOrder(order.Name)
		Expect(stored.Status.AddOnOperatorJobs).To(HaveLen(2))
		firstJobID := stored.Status.AddOnOperatorJobs[0].JobID
		secondJobID := stored.Status.AddOnOperatorJobs[1].JobID
		provider.setJobStatus(firstJobID, provisioning.ProvisionStatus{
			State:        osacv1alpha1.JobStateFailed,
			Message:      "installation failed",
			ErrorDetails: strings.Repeat("traceback ", 1000),
		})
		provider.setJobStatus(secondJobID, provisioning.ProvisionStatus{State: osacv1alpha1.JobStateSucceeded, Message: "installed"})

		result, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeNumerically(">", 0))
		stored = getOrder(order.Name)
		Expect(stored.Status.Phase).To(Equal(osacv1alpha1.ClusterOrderPhaseReady))
		Expect(stored.Status.AddOnOperatorJobs[0].State).To(Equal(osacv1alpha1.JobStateFailed))
		Expect(len(stored.Status.AddOnOperatorJobs[0].Message)).To(BeNumerically("<=", maxAddOnOperatorJobMessageLength))
		Expect(stored.Status.AddOnOperatorJobs[0].Message).NotTo(ContainSubstring("traceback"))
		Expect(stored.Status.AddOnOperatorJobs[1].State).To(Equal(osacv1alpha1.JobStateSucceeded))
		condition := findAddOnOperatorCondition(stored)
		Expect(condition).NotTo(BeNil())
		Expect(condition.Status).To(Equal(metav1.ConditionFalse))
		Expect(condition.Message).To(ContainSubstring("cert-manager"))

		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(provider.triggeredOperators).To(Equal([]string{"cert-manager", "gpu-operator"}))

		stored = getOrder(order.Name)
		stored.Status.AddOnOperatorJobs[0].Timestamp = metav1.NewTime(time.Now().Add(-provisioning.BackoffMaxDelay))
		Expect(k8sClient.Status().Update(ctx, stored)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(provider.triggeredOperators).To(Equal([]string{"cert-manager", "gpu-operator", "cert-manager"}))
		stored = getOrder(order.Name)
		Expect(stored.Status.AddOnOperatorJobs).To(HaveLen(3))
		Expect(findAddOnOperatorCondition(stored).Status).To(Equal(metav1.ConditionFalse))
		retryJobID := stored.Status.AddOnOperatorJobs[2].JobID
		provider.setJobStatus(retryJobID, provisioning.ProvisionStatus{State: osacv1alpha1.JobStateSucceeded, Message: "installed"})
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		stored = getOrder(order.Name)
		Expect(findAddOnOperatorCondition(stored).Status).To(Equal(metav1.ConditionTrue))
	})
})

func findAddOnOperatorCondition(order *osacv1alpha1.ClusterOrder) *metav1.Condition {
	return apimeta.FindStatusCondition(order.Status.Conditions, string(osacv1alpha1.ClusterOrderConditionAddOnOperatorsReady))
}
