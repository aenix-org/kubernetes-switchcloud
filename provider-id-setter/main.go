package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const imdsURL = "http://169.254.169.254/openstack/latest/meta_data.json"

type novaMetadata struct {
	UUID string            `json:"uuid"`
	Meta map[string]string `json:"meta"`
}

type nodePatch struct {
	Metadata nodePatchMeta `json:"metadata"`
	Spec     nodePatchSpec `json:"spec"`
}

type nodePatchMeta struct {
	Labels map[string]string `json:"labels,omitempty"`
}

type nodePatchSpec struct {
	ProviderID string `json:"providerID"`
}

func main() {
	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		slog.Error("NODE_NAME environment variable is not set")
		os.Exit(1)
	}

	meta, err := fetchIMDS()
	if err != nil {
		// Not an OpenStack node (OCI, bare-metal, etc.) — skip silently.
		slog.Info("IMDS not available, skipping providerID setup", "err", err)
		return
	}
	if meta.UUID == "" {
		slog.Info("IMDS returned empty UUID, skipping")
		return
	}

	providerID := "openstack:///" + meta.UUID
	instanceType := meta.Meta["instance-type"]
	slog.Info("fetched Nova metadata", "uuid", meta.UUID, "instanceType", instanceType)

	cfg, err := rest.InClusterConfig()
	if err != nil {
		slog.Error("in-cluster config", "err", err)
		os.Exit(1)
	}
	// Allow overriding the API server address for clusters where the kubernetes
	// service ClusterIP is not routable from all nodes (e.g. multi-cloud setups).
	if override := os.Getenv("KUBERNETES_API_SERVER_OVERRIDE"); override != "" {
		cfg.Host = override
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Error("creating kube client", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	node, err := client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		slog.Error("getting node", "node", nodeName, "err", err)
		os.Exit(1)
	}

	alreadyLabelled := node.Labels["node.kubernetes.io/instance-type"] != ""
	if node.Spec.ProviderID == providerID && alreadyLabelled {
		slog.Info("providerID and instance-type label already set, nothing to do", "providerID", providerID)
		return
	}

	patch := nodePatch{
		Spec: nodePatchSpec{ProviderID: providerID},
	}
	if instanceType != "" && !alreadyLabelled {
		patch.Metadata = nodePatchMeta{
			Labels: map[string]string{
				"node.kubernetes.io/instance-type": instanceType,
			},
		}
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		slog.Error("marshalling patch", "err", err)
		os.Exit(1)
	}

	if _, err := client.CoreV1().Nodes().Patch(
		ctx, nodeName,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{},
	); err != nil {
		slog.Error("patching node", "node", nodeName, "err", err)
		os.Exit(1)
	}

	slog.Info("node patched", "node", nodeName, "providerID", providerID, "instanceType", instanceType)
}

func fetchIMDS() (*novaMetadata, error) {
	resp, err := http.Get(imdsURL) //nolint:noctx
	if err != nil {
		return nil, fmt.Errorf("HTTP GET: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}

	var meta novaMetadata
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, fmt.Errorf("parsing JSON: %w", err)
	}

	return &meta, nil
}
