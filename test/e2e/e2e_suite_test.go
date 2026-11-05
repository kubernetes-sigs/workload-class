//go:build e2e

/*
Copyright 2026 The Kubernetes Authors.

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

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"sigs.k8s.io/workload-class/test/utils"
)

var (
	// managerImage is the manager image to be built and loaded for testing.
	managerImage = "example.com/workload-class:v0.0.1"
	// shouldCleanupCertManager tracks whether cert-manager was installed by this suite.
	shouldCleanupCertManager = false

	// namespace is the namespace the controller manager is deployed in.
	namespace = "workload-class-system"
)

func init() {
	if v, ok := os.LookupEnv("IMG"); ok && v != "" {
		managerImage = v
	}
}

// TestE2E runs the e2e test suite against an isolated Kind cluster.
// The default setup requires Kind and cert-manager (used to issue the webhook serving certificate).
//
// To skip the cert-manager installation, set: CERT_MANAGER_INSTALL_SKIP=true
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting workload-class e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager image")

	By("loading the manager image on Kind")
	err = utils.LoadImageToKindClusterWithName(managerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager image into Kind")

	setupCertManager()
})

var _ = AfterSuite(func() {
	teardownCertManager()
})

// setupCertManager installs cert-manager if needed for the webhook.
// Skips installation if CERT_MANAGER_INSTALL_SKIP=true or if it is already present.
func setupCertManager() {
	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == "true" {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping cert-manager installation (CERT_MANAGER_INSTALL_SKIP=true)\n")
		return
	}

	By("checking if cert-manager is already installed")
	if utils.IsCertManagerCRDsInstalled() {
		_, _ = fmt.Fprintf(GinkgoWriter, "cert-manager is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupCertManager = true

	By("installing cert-manager")
	Expect(utils.InstallCertManager()).To(Succeed(), "Failed to install cert-manager")
}

// teardownCertManager uninstalls cert-manager if it was installed by setupCertManager.
func teardownCertManager() {
	if !shouldCleanupCertManager {
		_, _ = fmt.Fprintf(GinkgoWriter, "Skipping cert-manager cleanup (not installed by this suite)\n")
		return
	}

	By("uninstalling cert-manager")
	utils.UninstallCertManager()
}
