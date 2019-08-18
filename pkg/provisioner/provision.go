package provisioner

import (
	"fmt"
	"os/exec"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/kubernetes-incubator/external-storage/lib/controller"
	"github.com/simt2/go-zfs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/pkg/api/v1"
)

// Provision creates a PersistentVolume, sets quota and shares it via NFS.
func (p ZFSProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	var serverHostname string
	if val, ok := options.Parameters["serverAddress"]; ok {
		serverHostname = val
	} else {
		hostname, err := exec.Command("hostname", "-f").Output()
		if err != nil {
			return nil, fmt.Errorf("Determining server hostname via \"hostname -f\" failed")
		}

		serverHostname = string(hostname)
	}

	kind := "nfs"

	if val, ok := options.Parameters["kind"]; ok {
		kind = val
	}

	path, err := p.createVolume(options)
	if err != nil {
		return nil, err
	}
	log.WithFields(log.Fields{
		"volume": path,
	}).Info("Created volume")

	// See nfs provisioner in github.com/kubernetes-incubator/external-storage for why we annotate this way and if it's still allowed
	annotations := make(map[string]string)
	annotations[annCreatedBy] = createdBy

	var volumeSource v1.PersistentVolumeSource

	switch kind {
	case "nfs":
		volumeSource = v1.PersistentVolumeSource{
			NFS: &v1.NFSVolumeSource{
				Server:   serverHostname,
				Path:     path,
				ReadOnly: false,
			},
		}
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        options.PVName,
			Labels:      map[string]string{},
			Annotations: annotations,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
			AccessModes:                   options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: volumeSource,
		},
	}
	log.Debug("Returning pv:")
	log.Debug(*pv)

	return pv, nil
}

// createVolume creates a ZFS dataset and returns its mount path
func (p ZFSProvisioner) createVolume(options controller.VolumeOptions) (string, error) {

	kind := "nfs"

	if val, ok := options.Parameters["kind"]; ok {
		kind = val
	}

	switch kind {
	case "nfs":
		zfsPath := p.parent.Name + "/" + options.PVName
		properties := make(map[string]string)

		properties["sharenfs"] = "rw=@10.0.0.0/8"

		if val, ok := options.Parameters["shareOptions"]; ok {
			properties["sharenfs"] = val
		}

		storageRequest := options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
		storageRequestBytes := strconv.FormatInt(storageRequest.Value(), 10)
		properties["refquota"] = storageRequestBytes
		properties["refreservation"] = storageRequestBytes

		dataset, err := zfs.CreateFilesystem(zfsPath, properties)
		if err != nil {
			return "", fmt.Errorf("Creating ZFS dataset failed with: %v", err.Error())
		}

		return dataset.Mountpoint, nil
	}

	return "", fmt.Errorf("Unknown volume kind: %s", kind)
}
