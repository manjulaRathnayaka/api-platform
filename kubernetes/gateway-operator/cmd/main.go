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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log/slog"
	"os"
	"strings"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	"github.com/go-logr/logr"
	apiv1 "github.com/wso2/api-platform/kubernetes/gateway-operator/api/v1"
	apiv1alpha1 "github.com/wso2/api-platform/kubernetes/gateway-operator/api/v1alpha1"
	"github.com/wso2/api-platform/kubernetes/gateway-operator/internal/config"
	"github.com/wso2/api-platform/kubernetes/gateway-operator/internal/controller"
	"github.com/wso2/api-platform/kubernetes/gateway-operator/pkg/logger"
	//+kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(apiv1.AddToScheme(scheme))
	utilruntime.Must(apiv1alpha1.AddToScheme(scheme))
	utilruntime.Must(gatewayv1.AddToScheme(scheme))
	utilruntime.Must(gatewayv1beta1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var configPath string

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&configPath, "config", "config/config.yaml", "Path to configuration file.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", false,
		"If set the metrics endpoint is served securely")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	flag.Parse()

	// Load operator configuration first to get logging config
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		// Use a plain fallback logger for config loading failure
		ctrl.SetLogger(logr.FromSlogHandler(slog.NewTextHandler(os.Stderr, nil)))
		setupLog.Error(err, "unable to load configuration")
		os.Exit(1)
	}

	// Build one shared slog handler/logger for both the controller-runtime
	// setup logger and the application/reconciler logger, so both produce
	// the same output shape.
	handler := logger.NewSlogHandler(logger.Config{
		Level:  cfg.Logging.Level,
		Format: cfg.Logging.Format,
	})
	appLog := slog.New(handler)
	ctrl.SetLogger(logr.FromSlogHandler(handler))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	tlsOpts := []func(*tls.Config){}
	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: tlsOpts,
	})

	// Parse WATCH_NAMESPACES env var
	watchNamespaces := os.Getenv("WATCH_NAMESPACES")
	var defaultNamespaces map[string]cache.Config
	if watchNamespaces != "" {
		nsList := strings.Split(watchNamespaces, ",")
		defaultNamespaces = make(map[string]cache.Config)
		for _, ns := range nsList {
			defaultNamespaces[strings.TrimSpace(ns)] = cache.Config{}
		}
		setupLog.Info("watching specific namespaces", "namespaces", nsList)
	} else {
		setupLog.Info("watching all namespaces (cluster-wide)")
	}

	// Helm stores every release revision as a Secret labelled owner=helm, holding the whole
	// rendered manifest. The operator talks to Helm through Helm's own client and never reads
	// these through the manager, so keep them out of the shared cache.
	notHelmRelease, err := labels.NewRequirement("owner", selection.NotEquals, []string{"helm"})
	if err != nil {
		setupLog.Error(err, "unable to build Secret cache selector")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{
			DefaultNamespaces: defaultNamespaces,
			// managedFields is never read by the operator and is often the largest part of
			// a cached object.
			DefaultTransform: cache.TransformStripManagedFields(),
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Secret{}: {Label: labels.NewSelector().Add(*notHelmRelease)},
			},
		},
		Controller: ctrlconfig.Controller{
			CacheSyncTimeout: cfg.Reconciliation.CacheSyncTimeout,
		},
		Metrics: metricsserver.Options{
			BindAddress:   metricsAddr,
			SecureServing: secureMetrics,
			TLSOpts:       tlsOpts,
		},
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "a5e4ae5b.gateway.api-platform.wso2.com",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// served reports whether every kind a controller watches is installed. A controller
	// whose CRD is missing would otherwise wait for a cache that can never sync and stop the
	// whole manager when CacheSyncTimeout expires.
	mapper := mgr.GetRESTMapper()
	served := func(objs ...client.Object) (bool, string) {
		for _, obj := range objs {
			gvk, err := apiutil.GVKForObject(obj, scheme)
			if err != nil {
				return false, err.Error()
			}
			if _, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version); err != nil {
				if meta.IsNoMatchError(err) {
					return false, gvk.String() + " is not installed"
				}
				setupLog.Error(err, "unable to check whether a kind is installed", "kind", gvk.String())
				os.Exit(1)
			}
		}
		return true, ""
	}

	resourceTracker := controller.NewResourceTracker()
	controllers := []struct {
		name     string
		requires []client.Object
		setup    func(ctrl.Manager) error
	}{
		{"APIGateway", []client.Object{&apiv1.APIGateway{}},
			controller.NewGatewayReconciler(mgr.GetClient(), mgr.GetScheme(), cfg, appLog).SetupWithManager},
		{"RestApi", []client.Object{&apiv1.RestApi{}, &apiv1.APIGateway{}},
			controller.NewRestApiReconciler(mgr.GetClient(), mgr.GetScheme(), cfg, appLog).SetupWithManager},
		{"K8sGateway", []client.Object{&gatewayv1.Gateway{}, &gatewayv1.HTTPRoute{}, &gatewayv1beta1.ReferenceGrant{}},
			controller.NewK8sGatewayReconciler(mgr.GetClient(), mgr.GetScheme(), cfg, appLog).SetupWithManager},
		{"K8sGatewayClass", []client.Object{&gatewayv1.GatewayClass{}},
			controller.NewK8sGatewayClassReconciler(mgr.GetClient(), mgr.GetScheme(), cfg, appLog).SetupWithManager},
		{"HTTPRoute", []client.Object{&gatewayv1.HTTPRoute{}, &gatewayv1.Gateway{}, &gatewayv1beta1.ReferenceGrant{}, &apiv1.APIPolicy{}},
			controller.NewHTTPRouteReconciler(mgr.GetClient(), mgr.GetScheme(), cfg, appLog).SetupWithManager},

		// Management-API resource controllers (WSO2 APIGateway flow only).
		// All share one in-memory ResourceTracker keyed by kind+namespace/name.
		{"LlmProviderTemplate", []client.Object{&apiv1.LlmProviderTemplate{}, &apiv1.APIGateway{}},
			controller.NewLlmProviderTemplateReconciler(mgr.GetClient(), cfg, appLog, resourceTracker).SetupWithManager},
		{"LlmProvider", []client.Object{&apiv1.LlmProvider{}, &apiv1.LlmProviderTemplate{}, &apiv1.APIGateway{}},
			controller.NewLlmProviderReconciler(mgr.GetClient(), cfg, appLog, resourceTracker).SetupWithManager},
		{"LlmProxy", []client.Object{&apiv1.LlmProxy{}, &apiv1.LlmProvider{}, &apiv1.APIGateway{}},
			controller.NewLlmProxyReconciler(mgr.GetClient(), cfg, appLog, resourceTracker).SetupWithManager},
		{"Mcp", []client.Object{&apiv1.Mcp{}, &apiv1.APIGateway{}},
			controller.NewMcpReconciler(mgr.GetClient(), cfg, appLog, resourceTracker).SetupWithManager},
		{"Agent", []client.Object{&apiv1.Agent{}, &apiv1.APIGateway{}},
			controller.NewAgentReconciler(mgr.GetClient(), cfg, appLog, resourceTracker).SetupWithManager},
		{"ManagedSecret", []client.Object{&apiv1.ManagedSecret{}, &apiv1.APIGateway{}},
			controller.NewManagedSecretReconciler(mgr.GetClient(), cfg, appLog, resourceTracker).SetupWithManager},
		{"Certificate", []client.Object{&apiv1.Certificate{}, &apiv1.APIGateway{}},
			controller.NewCertificateReconciler(mgr.GetClient(), cfg, appLog, resourceTracker).SetupWithManager},
		{"ApiKey", []client.Object{&apiv1.ApiKey{}, &apiv1.APIGateway{}},
			controller.NewApiKeyReconciler(mgr.GetClient(), cfg, appLog, resourceTracker).SetupWithManager},
		{"SubscriptionPlan", []client.Object{&apiv1.SubscriptionPlan{}, &apiv1.APIGateway{}},
			controller.NewSubscriptionPlanReconciler(mgr.GetClient(), cfg, appLog, resourceTracker).SetupWithManager},
		{"Subscription", []client.Object{&apiv1.Subscription{}, &apiv1.APIGateway{}},
			controller.NewSubscriptionReconciler(mgr.GetClient(), cfg, appLog, resourceTracker).SetupWithManager},
	}

	// Field indexes are registered only for installed kinds, for the same reason.
	var indexes []controller.FieldIndex
	for _, idx := range controller.FieldIndexes() {
		if ok, _ := served(idx.Object); ok {
			indexes = append(indexes, idx)
		}
	}
	if err := controller.RegisterFieldIndexes(context.Background(), mgr.GetFieldIndexer(), indexes); err != nil {
		setupLog.Error(err, "unable to register field indexes")
		os.Exit(1)
	}

	for _, c := range controllers {
		if !cfg.ControllerEnabled(c.name) {
			setupLog.Info("controller disabled by configuration", "controller", c.name)
			continue
		}
		if ok, reason := served(c.requires...); !ok {
			setupLog.Info("WARNING: controller not started because a kind it watches is not installed; restart the operator after installing it",
				"controller", c.name, "reason", reason)
			continue
		}
		if err := c.setup(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", c.name)
			os.Exit(1)
		}
	}
	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
