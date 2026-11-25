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

package config

import (
	"testing"
	"time"
)

func TestLoadConfig_ScaleDefaults(t *testing.T) {
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Reconciliation.CacheSyncTimeout != 5*time.Minute {
		t.Errorf("CacheSyncTimeout = %s, want 5m", cfg.Reconciliation.CacheSyncTimeout)
	}
	if cfg.Gateway.HelmMaxHistory != 3 {
		t.Errorf("HelmMaxHistory = %d, want 3", cfg.Gateway.HelmMaxHistory)
	}
	if !cfg.ControllerEnabled("RestApi") {
		t.Error("RestApi should be enabled by default")
	}
}

func TestLoadConfig_ScaleEnvOverrides(t *testing.T) {
	t.Setenv("GATEWAY_CACHE_SYNC_TIMEOUT", "12m")
	t.Setenv("GATEWAY_HELM_MAX_HISTORY", "0")
	t.Setenv("GATEWAY_DISABLED_CONTROLLERS", " RestApi, agent ,,Subscription")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Reconciliation.CacheSyncTimeout != 12*time.Minute {
		t.Errorf("CacheSyncTimeout = %s, want 12m", cfg.Reconciliation.CacheSyncTimeout)
	}
	if cfg.Gateway.HelmMaxHistory != 0 {
		t.Errorf("HelmMaxHistory = %d, want 0", cfg.Gateway.HelmMaxHistory)
	}
	for _, name := range []string{"RestApi", "Agent", "Subscription"} {
		if cfg.ControllerEnabled(name) {
			t.Errorf("%s should be disabled", name)
		}
	}
	for _, name := range []string{"HTTPRoute", "K8sGateway", "SubscriptionPlan"} {
		if !cfg.ControllerEnabled(name) {
			t.Errorf("%s should stay enabled", name)
		}
	}
}

func TestLoadConfig_ScaleEnvInvalid(t *testing.T) {
	for env, val := range map[string]string{
		"GATEWAY_CACHE_SYNC_TIMEOUT": "soon",
		"GATEWAY_HELM_MAX_HISTORY":   "-1",
	} {
		t.Run(env, func(t *testing.T) {
			t.Setenv(env, val)
			if _, err := LoadConfig(""); err == nil {
				t.Errorf("expected an error for %s=%s", env, val)
			}
		})
	}
}
