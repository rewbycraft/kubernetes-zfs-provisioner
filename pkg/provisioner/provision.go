package provisioner

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"path"
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
	case "iscsi":
		volumeSource = v1.PersistentVolumeSource{
			ISCSI: &v1.ISCSIVolumeSource{
				TargetPortal: serverHostname,
				IQN: path,
				Lun: 1,
				ReadOnly: false,
			},
		}
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("pvc-%s", options.PVC.UID),
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
		zfsPath := path.Join(p.parent.Name, fmt.Sprintf("pvc-%s", options.PVC.UID))
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
	case "iscsi":
		zfsPath := path.Join(p.parent.Name, fmt.Sprintf("pvc-%s", options.PVC.UID))
		storageRequest := options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
		iqn := fmt.Sprintf("%s:pvc-%s",options.Parameters["IQN"], options.PVC.UID)

		volumePath := path.Join("/dev/zvol/", zfsPath)

		tgtTemplate := `
<target %s>
     # Provided device as an iSCSI target
     backing-store %s
</target>
`

		tgtConfig := fmt.Sprintf(tgtTemplate, iqn, volumePath)

		err := ioutil.WriteFile(path.Join(p.tgtConfigDir, fmt.Sprintf("pvc-%s.conf", string(options.PVC.UID))), []byte(tgtConfig), 0644)
		if err != nil {
			return "", fmt.Errorf("Writing tgt config failed with: %v", err.Error())
		}

		_, err = zfs.CreateVolume(zfsPath, uint64(storageRequest.Value()), make(map[string]string))
		if err != nil {
			return "", fmt.Errorf("Creating ZFS volume failed with: %v", err.Error())
		}

		_, err = exec.Command("tgt-admin", "-e").Output()
		if err != nil {
			return "", fmt.Errorf("Updating tgtd failed.")
		}

		return iqn, nil
	}

	return "", fmt.Errorf("Unknown volume kind: %s", kind)
}
