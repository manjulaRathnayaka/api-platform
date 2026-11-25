/*
 *  Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package controller

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	apiv1 "github.com/wso2/api-platform/kubernetes/gateway-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayv1beta1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

func indexTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(s))
	utilruntime.Must(apiv1.AddToScheme(s))
	utilruntime.Must(gatewayv1.AddToScheme(s))
	utilruntime.Must(gatewayv1beta1.AddToScheme(s))
	return s
}

func routeWithBackend(ns, name, backendName string, backendNS *string) *gatewayv1.HTTPRoute {
	ref := gatewayv1.HTTPBackendRef{BackendRef: gatewayv1.BackendRef{
		BackendObjectReference: gatewayv1.BackendObjectReference{Name: gatewayv1.ObjectName(backendName)},
	}}
	if backendNS != nil {
		n := gatewayv1.Namespace(*backendNS)
		ref.Namespace = &n
	}
	return &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: gatewayv1.HTTPRouteSpec{Rules: []gatewayv1.HTTPRouteRule{{
			BackendRefs: []gatewayv1.HTTPBackendRef{ref, ref}, // duplicate is indexed once
		}}},
	}
}

func TestIndexHTTPRouteBackendServices(t *testing.T) {
	other := "shared"
	require.Equal(t, []string{"org-a/svc"}, indexHTTPRouteBackendServices(routeWithBackend("org-a", "r", "svc", nil)))
	require.Equal(t, []string{"shared/svc"}, indexHTTPRouteBackendServices(routeWithBackend("org-a", "r", "svc", &other)))
	require.Nil(t, indexHTTPRouteBackendServices(&corev1.Service{}))
}

func TestIndexHTTPRouteParentGateways(t *testing.T) {
	gwKind := gatewayv1.Kind("Gateway")
	svcKind := gatewayv1.Kind("Service")
	gwGroup := gatewayv1.Group(gatewayv1.GroupName)
	otherNS := gatewayv1.Namespace("infra")
	route := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "org-a", Name: "r"},
		Spec: gatewayv1.HTTPRouteSpec{CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{
			{Name: "gw-local"}, // defaults: kind/group/namespace unset
			{Name: "gw-infra", Namespace: &otherNS, Kind: &gwKind, Group: &gwGroup}, // explicit
			{Name: "gw-infra", Namespace: &otherNS},                                 // duplicate key
			{Name: "not-a-gateway", Kind: &svcKind},                                 // other kind is skipped
		}}},
	}
	require.ElementsMatch(t, []string{"org-a/gw-local", "infra/gw-infra"}, indexHTTPRouteParentGateways(route))
}

func TestIndexAPIPolicyValueFrom(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		"a": map[string]any{"valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "s1", "key": "k"}}},
		"b": map[string]any{"valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "s2", "namespace": "other", "key": "k"}}},
		"c": []any{map[string]any{"valueFrom": map[string]any{"configMapKeyRef": map[string]any{"name": "cm", "key": "k"}}}},
	})
	require.NoError(t, err)
	ap := &apiv1.APIPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: "p"},
		Spec:       apiv1.APIPolicySpec{Policies: []apiv1.Policy{{Name: "x", Version: "v1", Params: &runtime.RawExtension{Raw: raw}}}},
	}
	require.ElementsMatch(t, []string{"demo/s1", "other/s2"}, indexAPIPolicyValueFromSecrets(ap))
	require.ElementsMatch(t, []string{"demo/cm"}, indexAPIPolicyValueFromConfigMaps(ap))
}

func TestEnqueueHTTPRoutesForService_UsesIndex(t *testing.T) {
	shared := "shared"
	objs := []runtime.Object{
		routeWithBackend("org-a", "uses-svc", "svc", nil),
		routeWithBackend("org-b", "same-name-other-ns", "svc", nil),
		routeWithBackend("org-c", "cross-ns", "svc", &shared),
		routeWithBackend("org-a", "other-svc", "other", nil),
	}
	cl := newIndexedFakeClientBuilder(indexTestScheme()).WithRuntimeObjects(objs...).Build()
	r := &HTTPRouteReconciler{Client: cl}

	reqs := r.enqueueHTTPRoutesForService(context.Background(),
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "org-a", Name: "svc"}})
	require.Equal(t, []types.NamespacedName{{Namespace: "org-a", Name: "uses-svc"}}, requestKeys(reqs))

	reqs = r.enqueueHTTPRoutesForService(context.Background(),
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "shared", Name: "svc"}})
	require.Equal(t, []types.NamespacedName{{Namespace: "org-c", Name: "cross-ns"}}, requestKeys(reqs))

	require.Empty(t, r.enqueueHTTPRoutesForService(context.Background(),
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "org-z", Name: "svc"}}))
}

func TestEnqueueHTTPRoutesForGateway_UsesIndex(t *testing.T) {
	infra := gatewayv1.Namespace("infra")
	attached := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "org-a", Name: "attached"},
		Spec: gatewayv1.HTTPRouteSpec{CommonRouteSpec: gatewayv1.CommonRouteSpec{
			ParentRefs: []gatewayv1.ParentReference{{Name: "gw", Namespace: &infra}},
		}},
	}
	unattached := &gatewayv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "org-a", Name: "unattached"},
		Spec: gatewayv1.HTTPRouteSpec{CommonRouteSpec: gatewayv1.CommonRouteSpec{
			ParentRefs: []gatewayv1.ParentReference{{Name: "gw"}}, // org-a/gw, not infra/gw
		}},
	}
	cl := newIndexedFakeClientBuilder(indexTestScheme()).WithObjects(attached, unattached).Build()
	r := &HTTPRouteReconciler{Client: cl}

	reqs := r.enqueueHTTPRoutesForGateway(context.Background(),
		&gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "gw"}})
	require.Equal(t, []types.NamespacedName{{Namespace: "org-a", Name: "attached"}}, requestKeys(reqs))
}

func TestEnqueueHTTPRoutesForReferenceGrant_OnlyFromNamespaces(t *testing.T) {
	shared := "shared"
	inFrom := routeWithBackend("org-a", "in-from", "svc", &shared)
	notInFrom := routeWithBackend("org-b", "not-in-from", "svc", &shared)
	grant := &gatewayv1beta1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Namespace: "shared", Name: "allow-org-a"},
		Spec: gatewayv1beta1.ReferenceGrantSpec{
			From: []gatewayv1beta1.ReferenceGrantFrom{
				{Group: gatewayv1.GroupName, Kind: "HTTPRoute", Namespace: "org-a"},
				{Group: gatewayv1.GroupName, Kind: "HTTPRoute", Namespace: "org-a"}, // duplicate
			},
			To: []gatewayv1beta1.ReferenceGrantTo{{Group: "", Kind: "Service"}},
		},
	}
	cl := newIndexedFakeClientBuilder(indexTestScheme()).WithObjects(inFrom, notInFrom).Build()
	r := &HTTPRouteReconciler{Client: cl}

	reqs := r.enqueueHTTPRoutesForReferenceGrant(context.Background(), grant)
	require.Equal(t, []types.NamespacedName{{Namespace: "org-a", Name: "in-from"}}, requestKeys(reqs))
}

func requestKeys(reqs []reconcile.Request) []types.NamespacedName {
	out := make([]types.NamespacedName, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, r.NamespacedName)
	}
	return out
}
