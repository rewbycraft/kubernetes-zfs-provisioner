package provisioner

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"

	log "github.com/Sirupsen/logrus"
	zfs "github.com/simt2/go-zfs"
	"k8s.io/client-go/pkg/api/v1"
)

// Delete removes a given volume from the server
func (p ZFSProvisioner) Delete(volume *v1.PersistentVolume) error {
	err := p.deleteVolume(volume)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"volume": volume.Name,
	}).Info("Deleted volume")
	return nil
}

// deleteVolume deletes a ZFS dataset from the server
func (p ZFSProvisioner) deleteVolume(volume *v1.PersistentVolume) error {
	children, err := p.parent.Children(0)
	if err != nil {
		return fmt.Errorf("Retrieving ZFS dataset for deletion failed with: %v", err.Error())
	}

	if volume.Spec.ISCSI != nil {
		_, err = exec.Command("tgt-admin", "--delete", volume.Spec.ISCSI.IQN).Output()
		if err != nil {
			log.WithFields(log.Fields{
				"volume": volume.Name,
			}).Warnf("Updating tgtd failed: %y", err.Error())
		}

		tgtConfFile := path.Join(p.tgtConfigDir, fmt.Sprintf("%s.conf", string(volume.Name)))
		if _, err := os.Stat(tgtConfFile); err == nil {
			err = os.Remove(tgtConfFile)
			if err != nil {
				log.WithFields(log.Fields{
					"volume": volume.Name,
				}).Warnf("Removing tgt config failed: %y", err.Error())
			}
		}
	}

	var dataset *zfs.Dataset
	for _, child := range children {

		matched, _ := regexp.MatchString(`.+\/`+volume.Name, child.Name)
		if matched {
			dataset = child
			break
		}
	}

	if dataset == nil {
		log.WithFields(log.Fields{
			"volume": volume.Name,
		}).Warnf("Volume not found, so nothing to delete: %v", &volume)
		return nil
	}

	err = dataset.Destroy(zfs.DestroyRecursive)
	if err != nil {
		return fmt.Errorf("Deleting ZFS dataset failed with: %v", err.Error())
	}

	return nil
}
