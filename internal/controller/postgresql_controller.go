/*
Copyright 2024.

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

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	databasev1alpha1 "example.com/mini-operator/api/v1alpha1"
)

// PostgresqlReconciler reconciles a Postgresql object
type PostgresqlReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=database.example.com,resources=postgresqls,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=database.example.com,resources=postgresqls/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=database.example.com,resources=postgresqls/finalizers,verbs=update

func (r *PostgresqlReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Postgresql instance
	postgresql := &databasev1alpha1.Postgresql{}
	err := r.Get(ctx, req.NamespacedName, postgresql)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected.
			// Return and don't requeue
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return ctrl.Result{}, err
	}

	// Define the PostgreSQL deployment
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      postgresql.Name,
			Namespace: postgresql.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": postgresql.Name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": postgresql.Name},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "postgresql",
						Image: "postgres:latest",
						Ports: []corev1.ContainerPort{{
							ContainerPort: 5432,
							Name:          "postgresql",
						}},
						Env: []corev1.EnvVar{
							{
								Name:  "POSTGRES_USER",
								Value: postgresql.Spec.Username,
							},
							{
								Name:  "POSTGRES_PASSWORD",
								Value: postgresql.Spec.Password,
							},
							{
								Name:  "POSTGRES_DB",
								Value: postgresql.Spec.Dbname,
							},
						},
					}},
				},
			},
		},
	}

	// Set the owner reference to ensure garbage collection
	if err := controllerutil.SetControllerReference(postgresql, deployment, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	// Check if the deployment already exists
	found := &appsv1.Deployment{}
	err = r.Get(ctx, types.NamespacedName{Name: deployment.Name, Namespace: deployment.Namespace}, found)
	if err != nil && errors.IsNotFound(err) {
		logger.Info("Creating a new Deployment", "Deployment.Namespace", deployment.Namespace, "Deployment.Name", deployment.Name)
		err = r.Create(ctx, deployment)
		if err != nil {
			return ctrl.Result{}, err
		}
		// Deployment created successfully - requeue for further processing
		return ctrl.Result{Requeue: true}, nil
	} else if err != nil {
		return ctrl.Result{}, err
	}

	// Define the PostgreSQL service
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      postgresql.Name,
			Namespace: postgresql.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Port:     5432,
				Protocol: corev1.ProtocolTCP,
				Name:     "postgresql",
			}},
			Selector: map[string]string{"app": postgresql.Name},
			Type:     corev1.ServiceTypeClusterIP,
		},
	}

	// Set the owner reference to ensure garbage collection
	if err := controllerutil.SetControllerReference(postgresql, service, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	// Check if the service already exists
	foundService := &corev1.Service{}
	err = r.Get(ctx, types.NamespacedName{Name: service.Name, Namespace: service.Namespace}, foundService)
	if err != nil && errors.IsNotFound(err) {
		logger.Info("Creating a new Service", "Service.Namespace", service.Namespace, "Service.Name", service.Name)
		err = r.Create(ctx, service)
		if err != nil {
			return ctrl.Result{}, err
		}
	} else if err != nil {
		return ctrl.Result{}, err
	}

	// Execute SQL query
	if postgresql.Spec.Query != "" {
		err := r.execSQLQuery(ctx, postgresql)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PostgresqlReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&databasev1alpha1.Postgresql{}).
		Complete(r)
}

func int32Ptr(i int32) *int32 { return &i }

func (r *PostgresqlReconciler) execSQLQuery(ctx context.Context, postgresql *databasev1alpha1.Postgresql) error {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-exec-script", postgresql.Name),
			Namespace: postgresql.Namespace,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": postgresql.Name},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "busybox",
							Image:   "postgres:latest",
							Command: []string{"/bin/sh", "-c"},
							Args: []string{
								fmt.Sprintf(`while true; do
																pg_isready -h %s -p 5432
																if [ $? -ne 0 ]; then
																		echo "PostgreSQL is down! Executing command..."
																		psql -h %s -U %s -d %s -c "%s"
																		# 예시: curl -X POST http://<your-webhook-url>
																fi
																sleep 10
														done`,
									"postgresql-sample",
									"postgresql-sample",
									postgresql.Spec.Username,
									postgresql.Spec.Dbname,
									postgresql.Spec.Query),
							},
							Env: []corev1.EnvVar{
								{
									Name:  "PGPASSWORD",
									Value: postgresql.Spec.Password,
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}

	// Set the owner reference to ensure garbage collection
	if err := controllerutil.SetControllerReference(postgresql, job, r.Scheme); err != nil {
		return err
	}

	// Create the Job
	if err := r.Client.Create(ctx, job); err != nil {
		return err
	}

	return nil
}
