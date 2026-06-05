/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

// Package finalizers exports the finalizer-name constants used across
// the controller. Kept as a leaf package with no internal imports so
// both internal/controller (the reconciler that places the finalizer
// on Services) and internal/multicluster (the Session that filters
// Services by it during a KSC-change re-enqueue) can reference the
// same string without an import cycle.
package finalizers

// Service is the finalizer placed on every Service the controller
// claims responsibility for. Its presence on a Service object is
// the signal that there may still be an Octavia LB, FIP, or SG
// rule allocated for that Service in OpenStack and that the
// Service object must not be reaped from the apiserver until we
// have had a chance to clean them up.
const Service = "loadbalancer.switchcloud.aenix.io/cleanup"
