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
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// newIndexedFakeClientBuilder returns a fake client builder with the operator's field
// indexes registered for every kind the scheme knows, so indexed List calls behave as
// they do against the manager's cache.
func newIndexedFakeClientBuilder(scheme *runtime.Scheme) *fake.ClientBuilder {
	b := fake.NewClientBuilder().WithScheme(scheme)
	for _, idx := range FieldIndexes() {
		if _, err := apiutil.GVKForObject(idx.Object, scheme); err != nil {
			continue
		}
		b = b.WithIndex(idx.Object, idx.Field, idx.Extract)
	}
	return b
}
