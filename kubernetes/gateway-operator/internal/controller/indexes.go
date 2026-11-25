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
	"fmt"

	apiv1 "github.com/wso2/api-platform/kubernetes/gateway-operator/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// Field indexes let watch handlers look up the few objects an event affects instead of
// listing and deep-copying every object of the kind. A Service event used to copy every
// HTTPRoute in the cluster; with thousands of each, the initial sync alone did tens of
// millions of copies and could not finish inside the cache sync timeout.
const (
	// IndexHTTPRouteBackendService maps an HTTPRoute to "<namespace>/<name>" of each backendRef.
	IndexHTTPRouteBackendService = "gateway-operator.httproute.backendService"
	// IndexHTTPRouteParentGateway maps an HTTPRoute to "<namespace>/<name>" of each parent Gateway.
	IndexHTTPRouteParentGateway = "gateway-operator.httproute.parentGateway"
	// IndexAPIPolicyValueFromSecret maps an APIPolicy to "<namespace>/<name>" of each Secret its params reference.
	IndexAPIPolicyValueFromSecret = "gateway-operator.apipolicy.valueFromSecret"
	// IndexAPIPolicyValueFromConfigMap maps an APIPolicy to "<namespace>/<name>" of each ConfigMap its params reference.
	IndexAPIPolicyValueFromConfigMap = "gateway-operator.apipolicy.valueFromConfigMap"
)

// FieldIndex is one field index the operator registers on its cache.
type FieldIndex struct {
	Object  client.Object
	Field   string
	Extract client.IndexerFunc
}

// FieldIndexes returns every field index the watch handlers rely on. Tests register the
// same set on their fake clients so lookups behave as they do against the real cache.
func FieldIndexes() []FieldIndex {
	return []FieldIndex{
		{Object: &gatewayv1.HTTPRoute{}, Field: IndexHTTPRouteBackendService, Extract: indexHTTPRouteBackendServices},
		{Object: &gatewayv1.HTTPRoute{}, Field: IndexHTTPRouteParentGateway, Extract: indexHTTPRouteParentGateways},
		{Object: &apiv1.APIPolicy{}, Field: IndexAPIPolicyValueFromSecret, Extract: indexAPIPolicyValueFromSecrets},
		{Object: &apiv1.APIPolicy{}, Field: IndexAPIPolicyValueFromConfigMap, Extract: indexAPIPolicyValueFromConfigMaps},
	}
}

// RegisterFieldIndexes registers the given indexes on the manager's cache. It must run
// before the manager starts.
func RegisterFieldIndexes(ctx context.Context, indexer client.FieldIndexer, indexes []FieldIndex) error {
	for _, idx := range indexes {
		if err := indexer.IndexField(ctx, idx.Object, idx.Field, idx.Extract); err != nil {
			return fmt.Errorf("register field index %s: %w", idx.Field, err)
		}
	}
	return nil
}

func namespacedKey(namespace, name string) string {
	return namespace + "/" + name
}

// indexHTTPRouteBackendServices keys a route by every backendRef, resolved the same way as
// httpRouteReferencesService.
func indexHTTPRouteBackendServices(obj client.Object) []string {
	route, ok := obj.(*gatewayv1.HTTPRoute)
	if !ok {
		return nil
	}
	seen := make(map[string]struct{})
	var keys []string
	for _, rule := range route.Spec.Rules {
		for _, b := range rule.BackendRefs {
			ns := route.Namespace
			if b.Namespace != nil {
				ns = string(*b.Namespace)
			}
			k := namespacedKey(ns, string(b.Name))
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
	}
	return keys
}

// indexHTTPRouteParentGateways keys a route by every parentRef that targets a Gateway,
// resolved the same way as parentRefTargetsGateway.
func indexHTTPRouteParentGateways(obj client.Object) []string {
	route, ok := obj.(*gatewayv1.HTTPRoute)
	if !ok {
		return nil
	}
	seen := make(map[string]struct{})
	var keys []string
	for _, ref := range route.Spec.ParentRefs {
		if kind := normalizeParentRefKind(ref); kind != "" && kind != "Gateway" {
			continue
		}
		if group := normalizeParentRefGroup(ref); group != "" && group != gatewayv1.GroupName {
			continue
		}
		k := namespacedKey(normalizeParentRefNamespace(ref, route.Namespace), string(ref.Name))
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	return keys
}

// gatewayIndexKey is the IndexHTTPRouteParentGateway key for a Gateway, with the same
// empty-namespace default parentRefTargetsGateway applies.
func gatewayIndexKey(gw *gatewayv1.Gateway) string {
	ns := gw.Namespace
	if ns == "" {
		ns = "default"
	}
	return namespacedKey(ns, gw.Name)
}

func indexAPIPolicyValueFromSecrets(obj client.Object) []string {
	return apiPolicyValueFromKeys(obj, true)
}

func indexAPIPolicyValueFromConfigMaps(obj client.Object) []string {
	return apiPolicyValueFromKeys(obj, false)
}

func apiPolicyValueFromKeys(obj client.Object, secret bool) []string {
	ap, ok := obj.(*apiv1.APIPolicy)
	if !ok {
		return nil
	}
	ids := make(map[string]valueFromBackingID)
	for i := range ap.Spec.Policies {
		jsonTreeAccumulateBackingIDsFromPolicyParams(&ap.Spec.Policies[i], ap.Namespace, ids)
	}
	var keys []string
	for _, id := range ids {
		if id.secret == secret {
			keys = append(keys, namespacedKey(id.ns, id.name))
		}
	}
	return keys
}
