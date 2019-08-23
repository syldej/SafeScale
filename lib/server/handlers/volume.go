/*
 * Copyright 2018-2019, CS Systemes d'Information, http://www.c-s.fr
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package handlers

import (
	"context"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"math"
	"strconv"
	"strings"

	mapset "github.com/deckarep/golang-set"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/CS-SI/SafeScale/lib/server/iaas"
	"github.com/CS-SI/SafeScale/lib/server/iaas/resources"
	"github.com/CS-SI/SafeScale/lib/server/iaas/resources/enums/HostProperty"
	"github.com/CS-SI/SafeScale/lib/server/iaas/resources/enums/VolumeProperty"
	"github.com/CS-SI/SafeScale/lib/server/iaas/resources/enums/VolumeSpeed"
	propsv1 "github.com/CS-SI/SafeScale/lib/server/iaas/resources/properties/v1"
	"github.com/CS-SI/SafeScale/lib/server/metadata"
	"github.com/CS-SI/SafeScale/lib/system/nfs"
	"github.com/CS-SI/SafeScale/lib/utils"
	"github.com/CS-SI/SafeScale/lib/utils/retry"
)

//go:generate mockgen -destination=../mocks/mock_volumeapi.go -package=mocks github.com/CS-SI/SafeScale/lib/server/handlers VolumeAPI

// VolumeAPI defines API to manipulate hosts
type VolumeAPI interface {
	Delete(ctx context.Context, ref string) error
	List(ctx context.Context, all bool) ([]resources.Volume, error)
	Inspect(ctx context.Context, ref string) (*resources.Volume, map[string]*propsv1.HostLocalMount, error)
	Create(ctx context.Context, name string, size int, speed VolumeSpeed.Enum, inLvm bool, vusize int) (*resources.Volume, error)
	Attach(ctx context.Context, volume string, host string, path string, format string, doNotFormat bool) (string, error)
	Detach(ctx context.Context, volume string, host string) error
	Expand(ctx context.Context, volume string, host string, increment uint32, incrementType string) error
	Shrink(ctx context.Context, volume string, host string, increment uint32, incrementType string) error
}

// VolumeHandler volume service
type VolumeHandler struct {
	service iaas.Service
}

// NewVolumeHandler creates a Volume service
func NewVolumeHandler(svc iaas.Service) VolumeAPI {
	return &VolumeHandler{
		service: svc,
	}
}

// List returns the network list
func (handler *VolumeHandler) List(ctx context.Context, all bool) ([]resources.Volume, error) {
	if all {
		volumes, err := handler.service.ListVolumes()
		return volumes, infraErr(err)
	}

	var volumes []resources.Volume
	mv := metadata.NewVolume(handler.service)
	err := mv.Browse(func(volume *resources.Volume) error {
		volumes = append(volumes, *volume)
		return nil
	})
	if err != nil {
		return nil, infraErrf(err, "Error listing volumes")
	}
	return volumes, nil
}

// IMPORTANT TODO At service level, ve need to log before returning, because it's the last chance to track the real issue in server side

// Delete deletes volume referenced by ref
func (handler *VolumeHandler) Delete(ctx context.Context, ref string) error {
	mv, err := metadata.LoadVolume(handler.service, ref)
	if err != nil {
		switch err.(type) {
		case utils.ErrNotFound:
			return resources.ResourceNotFoundError("volume", ref)
		default:
			log.Debugf("Failed to delete volume: %+v", err)
			return infraErrf(err, "failed to delete volume")
		}
	}
	volume := mv.Get()

	err = volume.Properties.LockForRead(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
		volumeAttachmentsV1 := v.(*propsv1.VolumeAttachments)
		nbAttach := len(volumeAttachmentsV1.Hosts)
		if nbAttach > 0 {
			var list []string
			for _, v := range volumeAttachmentsV1.Hosts {
				list = append(list, v)
			}
			return logicErr(fmt.Errorf("still attached to %d host%s: %s", nbAttach, utils.Plural(nbAttach), strings.Join(list, ", ")))
		}
		return nil
	})
	if err != nil {
		return err
	}

	// FIXME Test volume deletion protection
	if volume.ManagedByLVM {
		if len(volume.PVM) == 0 {
			return logicErr(fmt.Errorf("volume belongs to a Logical Volume, cannot be deleted"))
		} else {
			// FIXME Delete everything...
			failures := false
			for _, volumeId := range volume.PVM {
				err = handler.service.DeleteVolume(volumeId.ID)
				if err != nil {
					log.Warnf("failure deleting volume %s", volumeId.ID)
					failures = true
				} else {
					pmv, err := metadata.LoadVolume(handler.service, volumeId.Name)
					if err != nil {
						log.Warnf("failure loading metadata of volume %s", volumeId.ID)
						failures = true
					} else {
						err = pmv.Delete()
						if err != nil {
							log.Warnf("failure deleting metadata of volume %s", volumeId.ID)
							failures = true
						}
					}
				}
			}
			if !failures {
				delErr := mv.Delete()
				return infraErr(delErr)
			}

			return logicErr(fmt.Errorf("There were some errors deleting the LVM %s, check the logs", ref))
		}
	} else { // Classic volumes
		err = handler.service.DeleteVolume(volume.ID)
		if err != nil {
			if _, ok := err.(resources.ErrResourceNotFound); !ok {
				return infraErrf(err, "can't delete volume")
			}
			log.Warnf("Unable to find the volume on provider side, cleaning up metadata")
		}
		err = mv.Delete()
		if err != nil {
			return infraErr(err)
		}
	}

	select {
	case <-ctx.Done():
		log.Warnf("Volume deletion cancelled by user")
		volumeBis, err := handler.Create(context.Background(), volume.Name, volume.Size, volume.Speed, volume.ManagedByLVM, volume.SizeVU)
		if err != nil {
			return fmt.Errorf("failed to stop volume deletion")
		}
		buf, err := volumeBis.Serialize()
		if err != nil {
			return fmt.Errorf("failed to recreate deleted volume")
		}
		return fmt.Errorf("deleted volume recreated by safescale : %s", buf)
	default:
	}

	return nil
}

// Get returns the volume identified by ref, ref can be the name or the id
func (handler *VolumeHandler) Get(ref string) (*resources.Volume, error) {
	mv, err := metadata.LoadVolume(handler.service, ref)
	if err != nil {
		if _, ok := err.(utils.ErrNotFound); ok {
			return nil, logicErr(resources.ResourceNotFoundError("volume", ref))
		}
		err := infraErr(err)
		return nil, err
	}
	if mv == nil {
		return nil, logicErr(resources.ResourceNotFoundError("volume", ref))
	}

	return mv.Get(), nil
}

// Inspect returns the volume identified by ref and its attachment (if any)
func (handler *VolumeHandler) Inspect(
	ctx context.Context,
	ref string,
) (*resources.Volume, map[string]*propsv1.HostLocalMount, error) {

	mv, err := metadata.LoadVolume(handler.service, ref)
	if err != nil {
		if _, ok := err.(utils.ErrNotFound); ok {
			return nil, nil, logicErr(resources.ResourceNotFoundError("volume", ref))
		}
		err := infraErr(err)
		return nil, nil, err
	}
	volume := mv.Get()

	mounts := map[string]*propsv1.HostLocalMount{}
	hostSvc := NewHostHandler(handler.service)

	err = volume.Properties.LockForRead(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
		volumeAttachedV1 := v.(*propsv1.VolumeAttachments)
		if len(volumeAttachedV1.Hosts) > 0 {
			for id := range volumeAttachedV1.Hosts {
				host, err := hostSvc.Inspect(ctx, id)
				if err != nil {
					continue
				}
				err = host.Properties.LockForRead(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
					hostVolumesV1 := v.(*propsv1.HostVolumes)
					if volumeAttachment, found := hostVolumesV1.VolumesByID[volume.ID]; found {
						err = host.Properties.LockForRead(HostProperty.MountsV1).ThenUse(func(v interface{}) error {
							hostMountsV1 := v.(*propsv1.HostMounts)
							if mount, ok := hostMountsV1.LocalMountsByPath[hostMountsV1.LocalMountsByDevice[volumeAttachment.Device]]; ok {
								mounts[host.Name] = mount
							} else {
								mounts[host.Name] = propsv1.NewHostLocalMount()
							}
							return nil
						})
						if err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					continue
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return volume, mounts, nil
}

// Create a volume
func (handler *VolumeHandler) Create(ctx context.Context, name string, size int, speed VolumeSpeed.Enum, inLVM bool, sizeVU int) (volume *resources.Volume, err error) {
	if inLVM {
		if sizeVU <= 0 {
			return nil, errors.New("VU size must be greater than 0")
		}

		numPVs := int(math.Ceil(float64(size) / float64(sizeVU)))
		log.Debugf("Create volume : creating %d PVs for volume %s", numPVs, name)

		var createdVolumes []*resources.Volume

		defer func() {
			if err != nil {
				log.Debug("Create volume cleanup: volume creation cleanup needed")

				for _, volToBeDeleted := range createdVolumes {
					derr := handler.service.DeleteVolume(volToBeDeleted.ID)
					log.Debugf("Create volume cleanup : volume deleted: %s", volToBeDeleted.Name)
					if derr != nil {
						log.Debugf("Create volume cleanup: failed to delete volume '%s': %v", volToBeDeleted.Name, derr)
					}
				}

			}
		}()

		// create PV first
		for subv := 0; subv < numPVs; subv++ {
			volume, err := handler.service.CreateVolume(resources.VolumeRequest{
				Name:   name + "_lvm_" + strconv.Itoa(subv),
				Size:   sizeVU,
				Speed:  speed,
				InLVM:  inLVM,
				SizeVU: sizeVU,
			})

			if err != nil {
				return nil, infraErr(err)
			}

			volume.ManagedByLVM = true
			createdVolumes = append(createdVolumes, volume)

			_, err = metadata.SaveVolume(handler.service, volume)
			if err != nil {
				log.Debugf("Create volume: error creating volume: saving volume metadata: %+v", err)
				return nil, infraErrf(err, "Create volume: error creating volume '%s' saving its volume metadata", name)
			}
		}

		// now store VG info as metadata only
		nvID, err := uuid.NewV4()
		_ = nvID

		nv := resources.NewVolume()
		nv.Name = name
		nv.Size = numPVs * sizeVU
		nv.ID = nvID.String()

		nv.ManagedByLVM = true
		nv.PVM = createdVolumes

		_, err = metadata.SaveVolume(handler.service, nv)
		if err != nil {
			log.Debugf("Create volume: error creating volume: saving volume metadata: %+v", err)
			return nil, infraErrf(err, "Create volume: error creating volume '%s' saving its volume metadata", name)
		}

		return nv, nil
	}

	volume, err = handler.service.CreateVolume(resources.VolumeRequest{
		Name:  name,
		Size:  size,
		Speed: speed,
	})
	if err != nil {
		return nil, infraErr(err)
	}
	defer func() {
		if err != nil {
			derr := handler.service.DeleteVolume(volume.ID)
			if derr != nil {
				log.Debugf("failed to delete volume '%s': %v", volume.Name, derr)
			}
		}
	}()

	md, err := metadata.SaveVolume(handler.service, volume)
	if err != nil {
		log.Debugf("Error creating volume: saving volume metadata: %+v", err)
		return nil, infraErrf(err, "error creating volume '%s' saving its metadata", name)
	}

	defer func() {
		if err != nil {
			derr := md.Delete()
			if derr != nil {
				log.Warnf("Failed to delete metadata of volume '%s'", volume.Name)
			}
		}
	}()

	select {
	case <-ctx.Done():
		log.Warnf("Volume creation cancelled by user")
		err = fmt.Errorf("volume creation cancelled by user")
		return nil, err
	default:
	}

	return volume, nil
}

func (handler *VolumeHandler) isAlreadyMounted(ctx context.Context, hostName string, volume *resources.Volume) (err error) {
	// Get Host data
	hostSvc := NewHostHandler(handler.service)
	host, err := hostSvc.ForceInspect(ctx, hostName)
	if err != nil {
		return throwErr(err)
	}

	err = host.Properties.LockForRead(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
		hostVolumesV1 := v.(*propsv1.HostVolumes)
		// Check if the volume is already mounted elsewhere
		if _, found := hostVolumesV1.DevicesByID[volume.ID]; found {
			return logicErr(fmt.Errorf("volume '%s' is already attached in '%s'", volume.Name, host.Name))
		}
		return nil
	})
	if err != nil {
		return infraErrf(err, "can't attach volume")
	}

	err = volume.Properties.LockForRead(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
		volumeAttachedV1 := v.(*propsv1.VolumeAttachments)
		if len(volumeAttachedV1.Hosts) > 0 {
			return logicErr(fmt.Errorf("volume '%s' is already attached", volume.Name))
		}
		return nil
	})
	if err != nil {
		return infraErrf(err, "can't get volume mount status")
	}

	return nil
}

// Attach a volume to an host
func (handler *VolumeHandler) Attach(ctx context.Context, volumeName, hostName, path, format string, doNotFormat bool) (string, error) {
	// Get volume data
	volume, _, err := handler.Inspect(ctx, volumeName)
	if err != nil {
		if _, ok := err.(resources.ErrResourceNotFound); ok {
			return "", err
		}
		return "", infraErr(err)
	}

	// FIXME Handle volume.Formatted
	if volume.ManagedByLVM {
		if len(volume.PVM) != 0 {
			if volume.Formatted {
				log.Debug("This should be managed by LVM and it's already formatted")
			} else {
				log.Debug("This should be managed by LVM")
			}
			return "", handler.attachLVM(ctx, volumeName, hostName, path, format, doNotFormat || volume.Formatted)
		}
	}

	// Get Host data
	hostSvc := NewHostHandler(handler.service)
	host, err := hostSvc.ForceInspect(ctx, hostName)
	if err != nil {
		return "", throwErr(err)
	}

	var (
		deviceName string
		volumeUUID string
		mountPoint string
		vaID       string
		server     *nfs.Server
	)

	err = volume.Properties.LockForWrite(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
		volumeAttachedV1 := v.(*propsv1.VolumeAttachments)

		mountPoint = path
		if path == resources.DefaultVolumeMountPoint {
			mountPoint = resources.DefaultVolumeMountPoint + volume.Name
		}

		// For now, allows only one attachment...
		if len(volumeAttachedV1.Hosts) > 0 {
			for id := range volumeAttachedV1.Hosts {
				if id != host.ID {
					return resources.ResourceNotAvailableError("volume", volumeName)
				}
				break
			}
		}

		return host.Properties.LockForWrite(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
			hostVolumesV1 := v.(*propsv1.HostVolumes)
			return host.Properties.LockForWrite(HostProperty.MountsV1).ThenUse(func(v interface{}) error {
				hostMountsV1 := v.(*propsv1.HostMounts)
				// Check if the volume is already mounted elsewhere
				if device, found := hostVolumesV1.DevicesByID[volume.ID]; found {
					mount, ok := hostMountsV1.LocalMountsByPath[hostMountsV1.LocalMountsByDevice[device]]
					if !ok {
						return logicErr(fmt.Errorf("metadata inconsistency for volume '%s' attached to host '%s'", volume.Name, host.Name))
					}
					path := mount.Path
					if path != mountPoint {
						return logicErr(fmt.Errorf("volume '%s' is already attached in '%s:%s'", volume.Name, host.Name, path))
					}
					return nil
				}

				// Check if there is no other device mounted in the path (or in subpath)
				for _, i := range hostMountsV1.LocalMountsByPath {
					if strings.Index(i.Path, mountPoint) == 0 {
						return logicErr(fmt.Errorf("can't attach volume '%s' to '%s:%s': there is already a volume mounted in '%s:%s'", volume.Name, host.Name, mountPoint, host.Name, i.Path))
					}
				}
				for _, i := range hostMountsV1.RemoteMountsByPath {
					if strings.Index(i.Path, mountPoint) == 0 {
						return logicErr(fmt.Errorf("can't attach volume '%s' to '%s:%s': there is a share mounted in path '%s:%s[/...]'", volume.Name, host.Name, mountPoint, host.Name, i.Path))
					}
				}

				// Note: most providers are not able to tell the real device name the volume
				//       will have on the host, so we have to use a way that can work everywhere
				// Get list of disks before attachment
				oldDiskSet, err := handler.listAttachedDevices(ctx, host)
				if err != nil {
					err := logicErrf(err, "failed to get list of connected disks")
					return err
				}
				vaID, err := handler.service.CreateVolumeAttachment(resources.VolumeAttachmentRequest{
					Name:     fmt.Sprintf("%s-%s", volume.Name, host.Name),
					HostID:   host.ID,
					VolumeID: volume.ID,
				})
				if err != nil {
					return infraErrf(err, "can't attach volume '%s'", volumeName)
				}
				// Starting from here, remove volume attachment if exit with error
				defer func() {
					if err != nil {
						derr := handler.service.DeleteVolumeAttachment(host.ID, vaID)
						if derr != nil {
							log.Errorf("failed to detach volume '%s' from host '%s': %v", volume.Name, host.Name, derr)
						}
					}
				}()

				// Updates volume properties
				volumeAttachedV1.Hosts[host.ID] = host.Name

				// Retries to acknowledge the volume is really attached to host
				var newDisk mapset.Set
				retryErr := retry.WhileUnsuccessfulDelay1Second(
					func() error {
						// Get new of disk after attachment
						newDiskSet, err := handler.listAttachedDevices(ctx, host)
						if err != nil {
							err := logicErrf(err, "failed to get list of connected disks")
							return err
						}
						// Isolate the new device
						newDisk = newDiskSet.Difference(oldDiskSet)
						if newDisk.Cardinality() == 0 {
							return logicErr(fmt.Errorf("disk not yet attached, retrying"))
						}
						return nil
					},
					utils.GetContextTimeout(),
				)
				if retryErr != nil {
					return logicErr(fmt.Errorf("failed to confirm the disk attachment after %s", utils.GetContextTimeout()))
				}

				// Recovers real device name from the system
				deviceName = "/dev/" + newDisk.ToSlice()[0].(string)

				// Create mount point
				sshHandler := NewSSHHandler(handler.service)
				sshConfig, err := sshHandler.GetConfig(ctx, host.ID)
				if err != nil {
					err = infraErr(err)
					return err
				}

				server, err = nfs.NewServer(sshConfig)
				if err != nil {
					return infraErr(err)
				}
				volumeUUID, err = server.MountBlockDevice(deviceName, mountPoint, format, doNotFormat)
				if err != nil {
					err = infraErr(err)
					return err
				}

				// Mark the volume as formatted
				if !doNotFormat {
					volume.Formatted = true
				}

				// Saves volume information in property
				hostVolumesV1.VolumesByID[volume.ID] = &propsv1.HostVolume{
					AttachID: vaID,
					Device:   volumeUUID,
				}
				hostVolumesV1.VolumesByName[volume.Name] = volume.ID
				hostVolumesV1.VolumesByDevice[volumeUUID] = volume.ID
				hostVolumesV1.DevicesByID[volume.ID] = volumeUUID

				// Starting from here, unmount block device if exiting with error
				defer func() {
					if err != nil {
						derr := server.UnmountBlockDevice(volumeUUID)
						if derr != nil {
							log.Errorf("failed to unmount volume '%s' from host '%s': %v", volume.Name, host.Name, derr)
						}
					}
				}()

				// Updates host properties
				hostMountsV1.LocalMountsByPath[mountPoint] = &propsv1.HostLocalMount{
					Device:     volumeUUID,
					Path:       mountPoint,
					FileSystem: "nfs",
				}
				hostMountsV1.LocalMountsByDevice[volumeUUID] = mountPoint
				log.Warnf("Storing in LocalMountsByDevice [%s], [%s]", volumeUUID, mountPoint)

				return nil
			})
		})
	})
	if err != nil {
		return "", infraErrf(err, "can't attach volume")
	}

	defer func() {
		if err != nil {
			derr := server.UnmountBlockDevice(volumeUUID)
			if derr != nil {
				log.Errorf("failed to unmount volume '%s' from host '%s': %v", volume.Name, host.Name, derr)
			}
			derr = handler.service.DeleteVolumeAttachment(host.ID, vaID)
			if derr != nil {
				log.Errorf("failed to detach volume '%s' from host '%s': %v", volume.Name, host.Name, derr)
			}
		}
	}()

	_, err = metadata.SaveVolume(handler.service, volume)
	if err != nil {
		return "", infraErrf(err, "can't attach volume")
	}

	defer func() {
		if err != nil {
			err2 := volume.Properties.LockForWrite(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
				volumeAttachedV1 := v.(*propsv1.VolumeAttachments)
				delete(volumeAttachedV1.Hosts, host.ID)
				return nil
			})
			if err2 != nil {
				log.Warnf("Failed to set volume %s metadatas", volumeName)
			}
			_, err2 = metadata.SaveVolume(handler.service, volume)
			if err2 != nil {
				log.Warnf("Failed to save volume %s metadatas", volumeName)
			}
		}
	}()

	mh, err := metadata.SaveHost(handler.service, host)
	if err != nil {
		return "", infraErrf(err, "can't attach volume")
	}

	defer func() {
		if err != nil {
			err2 := host.Properties.LockForWrite(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
				hostVolumesV1 := v.(*propsv1.HostVolumes)
				delete(hostVolumesV1.VolumesByID, volume.ID)
				delete(hostVolumesV1.VolumesByName, volume.Name)
				delete(hostVolumesV1.VolumesByDevice, volumeUUID)
				delete(hostVolumesV1.DevicesByID, volume.ID)
				return nil
			})
			if err2 != nil {
				log.Warnf("Failed to set host '%s' metadata about volumes", volumeName)
			}
			err2 = host.Properties.LockForWrite(HostProperty.MountsV1).ThenUse(func(v interface{}) error {
				hostMountsV1 := v.(*propsv1.HostMounts)
				delete(hostMountsV1.LocalMountsByDevice, volumeUUID)
				delete(hostMountsV1.LocalMountsByPath, mountPoint)
				return nil
			})
			if err2 != nil {
				log.Warnf("Failed to set host '%s' metadata about mounts", volumeName)
			}
			if mh.Write() != nil {
				log.Warnf("Failed to save host '%s' metadata", volumeName)
			}
		}
	}()

	select {
	case <-ctx.Done():
		log.Warnf("Volume attachment cancelled by user")
		err = fmt.Errorf("volume attachment cancelled by user")
		return "", err
	default:
	}

	log.Infof("Volume '%s' successfully attached to host '%s' as device '%s'", volume.Name, host.Name, volumeUUID)
	return deviceName, nil
}

func getHost(ctx context.Context, svc *VolumeHandler, hostName string) (*resources.Host, error) {
	// Load host data
	hostSvc := NewHostHandler(svc.service)
	host, err := hostSvc.ForceInspect(ctx, hostName)
	if err != nil {
		return nil, throwErr(err)
	}

	return host, nil
}

func getHostVolume(ctx context.Context, svc *VolumeHandler, hostName string) (*propsv1.HostVolumes, error) {
	// Load host data
	hostSvc := NewHostHandler(svc.service)
	host, err := hostSvc.ForceInspect(ctx, hostName)
	if err != nil {
		return nil, throwErr(err)
	}

	// Obtain volume attachment ID
	var hostVolumesV1 *propsv1.HostVolumes
	err = host.Properties.LockForRead(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
		hostVolumesV1 = v.(*propsv1.HostVolumes)
		return nil
	})
	if err != nil {
		return nil, infraErr(err)
	}

	return hostVolumesV1, nil
}

func (svc *VolumeHandler) attachLVM(ctx context.Context, volumeName, hostName, path, format string, doNotFormat bool) (err error) {
	// Get volume data
	volume, err := svc.Get(volumeName)
	if err != nil {
		switch err.(type) {
		case resources.ErrResourceNotFound:
			return infraErr(err)
		default:
			return infraErr(err)
		}
	}

	err = svc.isAlreadyMounted(ctx, hostName, volume)
	if err != nil {
		return throwErr(err)
	}

	var slicesAttachedSoFar []string

	defer func() {
		if err != nil {
			if len(slicesAttachedSoFar) != 0 {
				log.Debugln("attachLVM cleanup : detaching volumes after failure...")
			}
			for _, volSlice := range slicesAttachedSoFar {
				nerr := svc.Detach(ctx, volSlice, hostName)
				if nerr != nil {
					log.Debugf("attachLVM cleanup : error detaching volume %s", volSlice)
				}
			}
		}
	}()

	var deviceNames []string
	for _, volumeSlice := range volume.PVM {
		devName, err := svc.Attach(ctx, volumeSlice.Name, hostName, path, format, doNotFormat)
		if err != nil {
			return err
		}

		slicesAttachedSoFar = append(slicesAttachedSoFar, volumeSlice.Name)

		if devName != "" {
			deviceNames = append(deviceNames, devName)
		}
	}

	// Get Host data
	hostSvc := NewHostHandler(svc.service)
	host, err := hostSvc.ForceInspect(ctx, hostName)
	if err != nil {
		return throwErr(err)
	}

	// Create mount point
	sshHandler := NewSSHHandler(svc.service)
	sshConfig, err := sshHandler.GetConfig(ctx, host.ID)
	if err != nil {
		return infraErr(err)
	}

	server, err := nfs.NewServer(sshConfig)
	if err != nil {
		return infraErr(err)
	}

	outInfo, err := server.MountVGDevice("", volumeName, format, doNotFormat, deviceNames)
	if err != nil {
		return infraErr(err)
	}

	if !doNotFormat {
		var newIds []string

		for _, line := range strings.Split(outInfo, "\n") {
			if strings.HasPrefix(line, "SS:MOUNTED") {
				newIds = append(newIds, strings.Split(line, ":")[2])
			}
		}

		lvmId := newIds[len(newIds)-1]

		mountPoint := path
		if path == resources.DefaultVolumeMountPoint {
			mountPoint = resources.DefaultVolumeMountPoint + volume.Name
		}

		// Updates volume properties
		err = volume.Properties.LockForWrite(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
			volumeAttachedV1 := v.(*propsv1.VolumeAttachments)
			volumeAttachedV1.Hosts[host.ID] = host.Name
			return nil
		})
		if err != nil {
			return infraErrf(err, "can't attach volume")
		}

		for ind, volumeSlice := range volume.PVM {
			cuvol, err := svc.Get(volumeSlice.Name)
			if err != nil {
				return infraErr(err)
			}

			log.Debugf("Working with volume with ID %s", volumeSlice.ID)
			var previous string

			err = host.Properties.LockForWrite(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
				hostVolumesV1 := v.(*propsv1.HostVolumes)

				previous = hostVolumesV1.DevicesByID[volumeSlice.ID]
				log.Debugf("We should remove the local_mount_by_device with %s", previous)

				delete(hostVolumesV1.DevicesByID, volume.ID)
				log.Debugf("Updating UUID to %s", newIds[ind])
				err = hostVolumesV1.UpdateUUID(cuvol.ID, newIds[ind])
				if err != nil {
					return infraErr(err)
				}
				return nil
			})
			if err != nil {
				return infraErrf(err, "can't attach volume")
			}

			localMountPoint := mountPoint + "_lvm_" + strconv.Itoa(ind)
			err = host.Properties.LockForWrite(HostProperty.MountsV1).ThenUse(func(v interface{}) error {
				// Updates host properties
				hostMountsV1 := v.(*propsv1.HostMounts)

				hostMountsV1.LocalMountsByPath[localMountPoint] = &propsv1.HostLocalMount{
					Device:     newIds[ind],
					Path:       localMountPoint,
					FileSystem: format,
					Options:    "lvm",
				}

				delete(hostMountsV1.LocalMountsByDevice, previous)

				hostMountsV1.LocalMountsByDevice[newIds[ind]] = mountPoint + "_lvm_" + strconv.Itoa(ind)
				log.Warnf("Storing in LVM LocalMountsByDevice [%s], [%s]", newIds[ind], mountPoint+"_lvm_"+strconv.Itoa(ind))
				return nil
			})
			if err != nil {
				return infraErrf(err, "can't attach volume")
			}

			// FIXME Verfication
			err = host.Properties.LockForRead(HostProperty.MountsV1).ThenUse(func(v interface{}) error {
				// Updates host properties
				hostMountsV1 := v.(*propsv1.HostMounts)
				log.Warnf("Reading gives: %s", hostMountsV1.LocalMountsByPath[localMountPoint].Options)
				return nil
			})
			if err != nil {
				return infraErrf(err, "can't attach volume")
			}

			_, err = metadata.SaveHost(svc.service, host)
			if err != nil {
				return infraErrf(err, "can't attach volume")
			}
			// _ = sh.Write()
		}

		err = host.Properties.LockForWrite(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
			hostVolumesV1 := v.(*propsv1.HostVolumes)
			reset := volume.Name + "-" + host.Name
			hostVolumesV1.AddHostVolume(volume.ID, volume.Name, reset, lvmId)
			return nil
		})
		if err != nil {
			return infraErrf(err, "can't attach volume")
		}

		// FIXME new attachment ID problematic

		err = host.Properties.LockForWrite(HostProperty.MountsV1).ThenUse(func(v interface{}) error {
			hostMountsV1 := v.(*propsv1.HostMounts)
			// Updates host properties
			hostMountsV1.LocalMountsByPath[mountPoint] = &propsv1.HostLocalMount{
				Device:     lvmId,
				Path:       mountPoint,
				FileSystem: format,
				Options:    "lvm",
			}
			hostMountsV1.LocalMountsByDevice[lvmId] = mountPoint
			log.Warnf("Storing in LVM attachment LocalMountsByDevice [%s], [%s]", lvmId, mountPoint)
			return nil
		})
		if err != nil {
			return infraErrf(err, "can't attach volume")
		}

		_, err = metadata.SaveHost(svc.service, host)
		if err != nil {
			return infraErrf(err, "can't attach volume")
		}

		if !doNotFormat {
			volume.Formatted = true
		}

		_, err = metadata.SaveVolume(svc.service, volume)
		if err != nil {
			return infraErrf(err, "can't attach volume")
		}

		log.Debugf("Recovered info: [%s]", outInfo)
	} else {
		var lvmId string

		var newIds []string

		for _, line := range strings.Split(outInfo, "\n") {
			if strings.HasPrefix(line, "SS:MOUNTEDPV") {
				newIds = append(newIds, strings.Split(line, ":")[2])
			}
			if strings.HasPrefix(line, "SS:MOUNTEDLV") {
				lvmId = strings.Split(line, ":")[2]
			}
		}

		err = host.Properties.LockForWrite(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
			hostVolumesV1 := v.(*propsv1.HostVolumes)
			reset := volume.Name + "-" + host.Name
			hostVolumesV1.AddHostVolume(volume.ID, volume.Name, reset, lvmId)
			return nil
		})
		if err != nil {
			return infraErrf(err, "can't attach volume")
		}

		_, err = metadata.SaveHost(svc.service, host)
		if err != nil {
			return infraErrf(err, "can't attach volume")
		}

		err = volume.Properties.LockForWrite(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
			volumeAttachedV1 := v.(*propsv1.VolumeAttachments)
			// Updates volume properties
			volumeAttachedV1.Hosts[host.ID] = host.Name
			return nil
		})
		if err != nil {
			return infraErrf(err, "can't attach volume")
		}

		_, err = metadata.SaveVolume(svc.service, volume)
		if err != nil {
			return infraErrf(err, "can't attach volume")
		}

		// FIXME use functions to handle structs

		mountPoint := path
		if path == resources.DefaultVolumeMountPoint {
			mountPoint = resources.DefaultVolumeMountPoint + volume.Name
		}

		err = host.Properties.LockForWrite(HostProperty.MountsV1).ThenUse(func(v interface{}) error {
			hostMountsV1 := v.(*propsv1.HostMounts)
			for ind, volumeSlice := range volume.PVM {
				cuvol, err := svc.Get(volumeSlice.Name)
				if err != nil {
					return infraErr(err)
				}

				err = host.Properties.LockForWrite(HostProperty.VolumesV1).ThenUse(func(w interface{}) error {
					hostVolumesV1 := w.(*propsv1.HostVolumes)
					err = hostVolumesV1.UpdateUUID(cuvol.ID, newIds[ind])
					return err
				})
				if err != nil {
					return infraErr(err)
				}

				// Updates host properties
				hostMountsV1.LocalMountsByPath[mountPoint] = &propsv1.HostLocalMount{
					Device:     newIds[ind],
					Path:       mountPoint + "_lvm_" + strconv.Itoa(ind),
					FileSystem: format,
					Options:    "lvm",
				}
				hostMountsV1.LocalMountsByDevice[newIds[ind]] = mountPoint + "_lvm_" + strconv.Itoa(ind)
			}

			// Updates host properties
			hostMountsV1.LocalMountsByPath[mountPoint] = &propsv1.HostLocalMount{
				Device:     lvmId,
				Path:       mountPoint,
				FileSystem: format,
				Options:    "lvm",
			}
			hostMountsV1.LocalMountsByDevice[lvmId] = mountPoint
			return nil
		})
		if err != nil {
			return infraErrf(err, "can't attach volume")
		}

		_, err = metadata.SaveHost(svc.service, host)
		if err != nil {
			return infraErrf(err, "can't attach volume")
		}

		return nil
	}

	return nil
}

func (handler *VolumeHandler) listAttachedDevices(ctx context.Context, host *resources.Host) (set mapset.Set, err error) {
	var (
		retcode        int
		stdout, stderr string
	)
	cmd := "sudo lsblk -l -o NAME,TYPE | grep disk | cut -d' ' -f1"
	sshHandler := NewSSHHandler(handler.service)
	retryErr := retry.WhileUnsuccessfulDelay1Second(
		func() error {
			retcode, stdout, stderr, err = sshHandler.Run(ctx, host.ID, cmd)
			if err != nil {
				err = infraErr(err)
				return err
			}
			if retcode != 0 {
				if retcode == 255 {
					return fmt.Errorf("failed to reach SSH service of host '%s', retrying", host.Name)
				}
				return fmt.Errorf(stderr)
			}
			return nil
		},
		utils.GetContextTimeout(),
	)
	if retryErr != nil {
		return nil, logicErrf(retryErr, fmt.Sprintf("failed to get list of connected disks after %s", utils.GetContextTimeout()))
	}
	disks := strings.Split(stdout, "\n")
	set = mapset.NewThreadUnsafeSet()
	for _, k := range disks {
		set.Add(k)
	}
	return set, nil
}

func getServer(ctx context.Context, handler *VolumeHandler, hostName string) (*nfs.Server, error) {
	// Get Host data
	hostSvc := NewHostHandler(handler.service)
	host, err := hostSvc.ForceInspect(ctx, hostName)
	if err != nil {
		return nil, infraErr(err)
	}

	sshHandler := NewSSHHandler(handler.service)
	sshConfig, err := sshHandler.GetConfig(ctx, host.ID)
	if err != nil {
		return nil, logicErrf(err, "error getting ssh config")
	}
	server, err := nfs.NewServer(sshConfig)
	if err != nil {
		return nil, infraErr(err)
	}

	return server, nil
}

func getServerByID(ctx context.Context, handler *VolumeHandler, hostID string) (server *nfs.Server, err error) {
	sshHandler := NewSSHHandler(handler.service)
	sshConfig, err := sshHandler.GetConfig(ctx, hostID)
	if err != nil {
		return nil, logicErrf(err, "error getting ssh config")
	}
	server, err = nfs.NewServer(sshConfig)
	if err != nil {
		return nil, infraErr(err)
	}

	return server, nil
}

func getHostLocalMount(ctx context.Context, handler *VolumeHandler, volumeName, hostName string) (mount *propsv1.HostLocalMount, err error) {
	// Get Host data
	hostSvc := NewHostHandler(handler.service)
	host, err := hostSvc.ForceInspect(ctx, hostName)
	if err != nil {
		return nil, throwErr(err)
	}

	volume, err := handler.Get(volumeName)
	if err != nil {
		switch err.(type) {
		case resources.ErrResourceNotFound:
			return nil, infraErr(err)
		default:
			return nil, infraErr(resources.ResourceNotFoundError("volume", volumeName))
		}
	}

	var attachment *propsv1.HostVolume

	// Obtain volume attachment ID
	err = host.Properties.LockForRead(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
		hostVolumesV1 := v.(*propsv1.HostVolumes)
		att, found := hostVolumesV1.VolumesByID[volume.ID]
		if !found {
			return logicErr(fmt.Errorf("Can't detach volume '%s': not attached to host '%s'", volumeName, host.Name))
		}
		attachment = att
		return nil
	})
	if err != nil {
		return nil, infraErr(err)
	}

	// Obtain mounts information
	err = host.Properties.LockForRead(HostProperty.MountsV1).ThenUse(func(v interface{}) error {
		hostMountsV1 := v.(*propsv1.HostMounts)
		device := attachment.Device
		path := hostMountsV1.LocalMountsByDevice[device]
		mount = hostMountsV1.LocalMountsByPath[path]
		if mount == nil {
			return logicErr(errors.Wrap(fmt.Errorf("metadata inconsistency: no mount corresponding to volume attachment"), ""))
		}
		return nil
	})
	if err != nil {
		return nil, infraErr(err)
	}

	return mount, nil
}

// Detach detach the volume identified by ref, ref can be the name or the id
func (svc *VolumeHandler) Expand(ctx context.Context, volumeName, hostName string, increment uint32, incrementType string) (err error) {
	// Load volume data
	volume, err := svc.Get(volumeName)
	if err != nil {
		switch err.(type) {
		case resources.ErrResourceNotFound:
			return infraErr(err)
		default:
			return infraErr(resources.ResourceNotFoundError("volume", volumeName))
		}
	}

	if !volume.ManagedByLVM {
		return logicErr(fmt.Errorf("Standard volumes cannot be expanded"))
	}

	if len(volume.PVM) == 0 {
		return logicErr(fmt.Errorf("Physical volumes cannot be expanded, only the volume group can be expanded"))
	}

	vuSize := volume.Size / len(volume.PVM)
	valids := []string{"gb", "uv", "ratio"}
	validChange := false
	for _, item := range valids {
		if incrementType == item {
			validChange = true
		}
	}

	// that should never happen, consider panicking instead
	if !validChange {
		return logicErr(fmt.Errorf("Unknown size parameter"))
	}

	nun := uint32(0)

	if incrementType == "gb" {
		nun = uint32(math.Ceil(float64(float64(increment) / float64(vuSize))))
		log.Debugf("We have to create volumes of %d Gb, working with units of %d Gb size, %d volumes", increment, vuSize, nun)
	}

	if incrementType == "uv" {
		nun = increment
		log.Debugf("We have to add %d volumes of %d Gb each", increment, vuSize)
	}

	if incrementType == "ratio" {
		targetVol := float64(volume.Size) * float64(increment) / (100 * float64(vuSize))
		nun = uint32(math.Ceil(float64(targetVol)))
		log.Debugf("After some maths, we need to add %d volumes of %d Gb each", nun, vuSize)
	}

	mountInfo, err := getHostLocalMount(ctx, svc, volumeName, hostName)
	if err != nil {
		return throwErr(err)
	}
	log.Debugf("Volume path [%s], filesystem [%s]", mountInfo.Path, mountInfo.FileSystem)

	var deviceNames []string
	var createdVolumes []*resources.Volume

	defer func() {
		if err != nil {
			log.Debugf("Expand cleanup : cleaning volumes...")
			for _, vol := range createdVolumes {
				errRemovingVolume := svc.service.DeleteVolume(vol.ID)
				if errRemovingVolume != nil {
					log.Debugf("Expand cleanup : error removing volume: %s", errRemovingVolume.Error())
				} else {
					errorDeletingVolume := metadata.RemoveVolume(svc.service, vol.ID)
					if errorDeletingVolume != nil {
						log.Debugf("Expand cleanup : error removing volume metadata: %s", errorDeletingVolume.Error())
					} else {
						log.Debugf("Expand cleanup : cleaned volume %s", vol.Name)
					}
				}
			}
		}
	}()

	for tba := 0; tba < int(nun); tba++ {

		newVolume, err := svc.service.CreateVolume(resources.VolumeRequest{
			Name:   volume.Name + "_lvm_" + strconv.Itoa(len(volume.PVM)+tba),
			Size:   vuSize,
			Speed:  volume.Speed,
			InLVM:  true,
			SizeVU: vuSize,
		})

		if err != nil {
			return infraErr(err)
		}

		newVolume.ManagedByLVM = true
		createdVolumes = append(createdVolumes, newVolume)

		_, err = metadata.SaveVolume(svc.service, newVolume)
		if err != nil {
			log.Debugf("Error creating volume: saving volume metadata: %+v", err)
			return infraErrf(err, "Error creating volume '%s' saving its volume metadata", newVolume.Name)
		}

		devName, err := svc.Attach(ctx, newVolume.Name, hostName, mountInfo.Path+"_lvm_"+strconv.Itoa(len(volume.PVM)+tba), mountInfo.FileSystem, false)
		if err != nil {
			log.Debugf("Error attaching volume: %v", err.Error())
			return err
		}
		if devName != "" {
			deviceNames = append(deviceNames, devName)
		}
	}

	server, err := getServer(ctx, svc, hostName)
	if err != nil {
		return throwErr(err)
	}

	// FIXME Use recovered info from string
	_, err = server.ExpandVGDevice("", volumeName, mountInfo.FileSystem, false, deviceNames)
	if err != nil {
		return infraErr(err)
	}

	for _, addedVol := range createdVolumes {
		volume.PVM = append(volume.PVM, addedVol)
		volume.Size = volume.Size + addedVol.Size
	}

	_, err = metadata.SaveVolume(svc.service, volume)
	if err != nil {
		log.Debugf("Error creating volume: saving volume metadata: %+v", err)
		return infraErrf(err, "Error creating volume '%s' saving its volume metadata", volume.Name)
	}

	return nil
}

// Detach detach the volume identified by ref, ref can be the name or the id
func (svc *VolumeHandler) Shrink(ctx context.Context, volumeName, hostName string, increment uint32, incrementType string) (err error) {
	// Load volume data
	volume, err := svc.Get(volumeName)
	if err != nil {
		switch err.(type) {
		case resources.ErrResourceNotFound:
			return infraErr(err)
		default:
			return infraErr(resources.ResourceNotFoundError("volume", volumeName))
		}
	}

	if !volume.ManagedByLVM {
		return logicErr(fmt.Errorf("Standard volumes cannot be shrinked"))
	}

	if len(volume.PVM) == 0 {
		return logicErr(fmt.Errorf("Physical volumes cannot be shrinked, only the group can be shrinked"))
	}

	vuSize := volume.Size / len(volume.PVM)
	valids := []string{"gb", "uv", "ratio"}
	validChange := false
	for _, item := range valids {
		if incrementType == item {
			validChange = true
		}
	}

	if !validChange {
		return logicErr(fmt.Errorf("Unknown size parameter"))
	}

	numberOfVolumeUnitsAffected := uint32(0)
	wantedSizeInGb := volume.Size

	if incrementType == "gb" {
		numberOfVolumeUnitsAffected = uint32(math.Ceil(float64(float64(increment) / float64(vuSize))))
		log.Debugf("We have to remove volumes of %d Gb, working with units of %d Gb size, %d volumes", increment, vuSize, numberOfVolumeUnitsAffected)
		wantedSizeInGb = wantedSizeInGb - int(increment)
	}

	if incrementType == "uv" {
		numberOfVolumeUnitsAffected = increment
		log.Debugf("We have to add %d volumes of %d Gb each", increment, vuSize)
		wantedSizeInGb = wantedSizeInGb - (int(increment) * int(vuSize))
	}

	if incrementType == "ratio" {
		targetVol := float64(volume.Size) * float64(increment) / (100 * float64(vuSize))
		numberOfVolumeUnitsAffected = uint32(math.Ceil(float64(targetVol)))
		wantedSizeInGb = wantedSizeInGb - int(float64(volume.Size)*float64(increment)/float64(100))
	}

	if incrementType == "" {
		log.Debugf("Resize to a minimum by default")
	}

	log.Debugf("We have a target size from %d to %d Gb", volume.Size, wantedSizeInGb)

	mountInfo, err := getHostLocalMount(ctx, svc, volumeName, hostName)
	if err != nil {
		return throwErr(err)
	}
	log.Debugf("Volume path [%s], filesystem [%s]", mountInfo.Path, mountInfo.FileSystem)

	var deviceNames []string

	server, err := getServer(ctx, svc, hostName)
	if err != nil {
		return throwErr(err)
	}

	shrinkOutput, err := server.ShrinkVGDevice("", volumeName, mountInfo.FileSystem, false, deviceNames, vuSize, wantedSizeInGb)
	if err != nil {
		if strings.Contains(shrinkOutput, "SS:FAILURE:") {
			lines := strings.Split(shrinkOutput, "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "SS:FAILURE:") {
					fragments := strings.Split(line, ":")
					return infraErrf(err, fragments[2])
				}
			}

		}
		return infraErr(err)
	}

	// FIXME Rename structs
	type smp struct {
		uuid       string
		mountpoint string
	}

	var points []smp

	lines := strings.Split(shrinkOutput, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "SS:DELETED:") {
			log.Debugf("We had some shrink output: [%s]", line)
			fragments := strings.Split(line, ":")
			points = append(points, smp{uuid: fragments[2], mountpoint: fragments[3]})
		}
	}

	host, err := getHost(ctx, svc, hostName)
	if err != nil {
		return throwErr(err)
	}

	hostVolumesV1, err := getHostVolume(ctx, svc, hostName)
	if err != nil {
		return throwErr(err)
	}

	for _, point := range points {
		vod, ok := hostVolumesV1.VolumesByDevice[point.uuid]
		if ok {
			hovol := hostVolumesV1.VolumesByID[vod]
			log.Debugf("We have to delete : [%s], attachment [%s]", hovol.Device, hovol.AttachID)
			errRemovingAttachment := svc.service.DeleteVolumeAttachment(host.ID, hovol.AttachID)

			failed := false
			if errRemovingAttachment != nil {
				failed = true
				log.Debugf("Error removing volume attachment: %s", errRemovingAttachment.Error())
			} else {
				log.Debugf("Removed volume attachment [%s]", hovol.AttachID)
			}
			errRemovingVolume := svc.service.DeleteVolume(vod)
			if errRemovingVolume != nil {
				failed = true
				log.Debugf("Error removing volume: %s", errRemovingVolume.Error())
			} else {
				log.Debugf("Removed volume [%s]", vod)
			}

			if !failed {
				// update volume PVMs
				var na []*resources.Volume
				for _, v := range volume.PVM {
					if v.ID == vod {
						continue
					} else {
						na = append(na, v)
					}
				}
				volume.PVM = na
				volume.Size = volume.Size - vuSize
			}
		}
	}

	_, err = metadata.SaveVolume(svc.service, volume)
	if err != nil {
		log.Debugf("Error creating volume: saving volume metadata: %+v", err)
		return infraErrf(err, "Error creating volume '%s' saving its volume metadata", volume.Name)
	}

	return nil
}

// Detach detach the volume identified by ref, ref can be the name or the id
func (handler *VolumeHandler) Detach(ctx context.Context, volumeName, hostName string) error {
	// Load volume data
	volume, _, err := handler.Inspect(ctx, volumeName)
	if err != nil {
		if _, ok := err.(resources.ErrResourceNotFound); !ok {
			return infraErr(err)
		}
		return infraErr(resources.ResourceNotFoundError("volume", volumeName))
	}
	mountPath := ""

	// FIXME Here goes PVM Managements

	if volume.ManagedByLVM {
		if len(volume.PVM) != 0 {
			log.Debug("This should be managed by LVM")
			return handler.detachLVM(ctx, volumeName, hostName)
		}
	}

	// Load host data
	hostSvc := NewHostHandler(handler.service)
	host, err := hostSvc.ForceInspect(ctx, hostName)
	if err != nil {
		return throwErr(err)
	}

	// Obtain volume attachment ID
	err = host.Properties.LockForWrite(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
		hostVolumesV1 := v.(*propsv1.HostVolumes)

		// Check the volume is effectively attached
		attachment, found := hostVolumesV1.VolumesByID[volume.ID]
		if !found {
			return logicErr(fmt.Errorf("Can't detach volume '%s': not attached to host '%s'", volumeName, host.Name))
		}

		// Obtain mounts information
		return host.Properties.LockForWrite(HostProperty.MountsV1).ThenUse(func(v interface{}) error {
			hostMountsV1 := v.(*propsv1.HostMounts)
			device := attachment.Device
			mountPath = hostMountsV1.LocalMountsByDevice[device]
			mount := hostMountsV1.LocalMountsByPath[mountPath]
			if mount == nil {
				return logicErr(errors.Wrap(fmt.Errorf("metadata inconsistency: no mount corresponding to volume attachment"), ""))
			}

			// Check if volume has other mount(s) inside it
			for p, i := range hostMountsV1.LocalMountsByPath {
				if i.Device == device {
					continue
				}
				if strings.Index(p, mount.Path) == 0 {
					return logicErr(fmt.Errorf("can't detach volume '%s' from '%s:%s', there is a volume mounted in '%s:%s'",
						volume.Name, host.Name, mount.Path, host.Name, p))
				}
			}
			for p := range hostMountsV1.RemoteMountsByPath {
				if strings.Index(p, mount.Path) == 0 {
					return logicErr(fmt.Errorf("can't detach volume '%s' from '%s:%s', there is a share mounted in '%s:%s'",
						volume.Name, host.Name, mount.Path, host.Name, p))
				}
			}

			// Check if volume (or a subdir in volume) is shared
			return host.Properties.LockForWrite(HostProperty.SharesV1).ThenUse(func(v interface{}) error {
				hostSharesV1 := v.(*propsv1.HostShares)

				for _, v := range hostSharesV1.ByID {
					if strings.Index(v.Path, mount.Path) == 0 {
						return logicErr(fmt.Errorf("can't detach volume '%s' from '%s:%s', '%s:%s' is shared",
							volume.Name, host.Name, mount.Path, host.Name, v.Path))
					}
				}

				// Unmount the Block Device ...
				sshHandler := NewSSHHandler(handler.service)
				sshConfig, err := sshHandler.GetConfig(ctx, host.ID)
				if err != nil {
					err = logicErrf(err, "error getting ssh config")
					return err
				}
				nfsServer, err := nfs.NewServer(sshConfig)
				if err != nil {
					err = logicErrf(err, "error creating nfs service")
					return err
				}
				err = nfsServer.UnmountBlockDevice(attachment.Device)
				if err != nil {
					// FIXME Warning here
					_ = logicErrf(err, "error unmounting block device")
					//return err
				}

				// ... then detach volume
				err = handler.service.DeleteVolumeAttachment(host.ID, attachment.AttachID)
				if err != nil {
					err = infraErr(err)
					return err
				}

				// Updates host property propsv1.VolumesV1
				delete(hostVolumesV1.VolumesByID, volume.ID)
				delete(hostVolumesV1.VolumesByName, volume.Name)
				delete(hostVolumesV1.VolumesByDevice, attachment.Device)
				delete(hostVolumesV1.DevicesByID, volume.ID)

				// Updates host property propsv1.MountsV1
				delete(hostMountsV1.LocalMountsByDevice, mount.Device)
				delete(hostMountsV1.LocalMountsByPath, mount.Path)

				// Updates volume property propsv1.VolumeAttachments
				return volume.Properties.LockForWrite(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
					volumeAttachedV1 := v.(*propsv1.VolumeAttachments)
					delete(volumeAttachedV1.Hosts, host.ID)
					return nil
				})
			})
		})
	})
	if err != nil {
		return err
	}

	// Updates metadata
	_, err = metadata.SaveHost(handler.service, host)
	if err != nil {
		return infraErr(err)
	}
	_, err = metadata.SaveVolume(handler.service, volume)
	if err != nil {
		err = infraErr(err)
		return err
	}

	select {
	case <-ctx.Done():
		log.Warnf("Volume detachment cancelled by user")
		// Currently format is not registered anywhere so we use ext4 the most common format (but as we mount the volume the format parameter is ignored anyway)
		_, err = handler.Attach(context.Background(), volumeName, hostName, mountPath, "ext4", true)
		if err != nil {
			return fmt.Errorf("failed to stop volume detachment")
		}
		return fmt.Errorf("volume detachment canceld by user")
	default:
	}

	return nil
}

func (handler *VolumeHandler) detachLVM(ctx context.Context, volumeName, hostName string) (err error) {
	// Load volume data
	volume, err := handler.Get(volumeName)
	if err != nil {
		switch err.(type) {
		case resources.ErrResourceNotFound:
			return infraErr(err)
		default:
			return infraErr(resources.ResourceNotFoundError("volume", volumeName))
		}
	}

	// Load host data
	hostSvc := NewHostHandler(handler.service)
	host, err := hostSvc.ForceInspect(ctx, hostName)
	if err != nil {
		return throwErr(err)
	}

	// Obtain volume attachment ID
	var hostVolumesV1 *propsv1.HostVolumes
	err = host.Properties.LockForRead(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
		hostVolumesV1 = v.(*propsv1.HostVolumes)
		return nil
	})
	if err != nil {
		return infraErr(err)
	}

	// FIXME Improve this part for LVM

	// Check the volume is effectively attached
	attachment, found := hostVolumesV1.VolumesByID[volume.ID]
	if !found {
		return logicErr(fmt.Errorf("Can't detach volume '%s': not attached to host '%s'", volumeName, host.Name))
	}

	var volumeAttachedV1 *propsv1.VolumeAttachments
	err = volume.Properties.LockForRead(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
		volumeAttachedV1 = v.(*propsv1.VolumeAttachments)
		return nil
	})
	if err != nil {
		return infraErrf(err, "can't get volume mount status")
	}
	if len(volumeAttachedV1.Hosts) == 0 {
		return logicErr(fmt.Errorf("volume '%s' is already detached", volume.Name))
	}

	// Unmount the Block Device ...
	sshHandler := NewSSHHandler(handler.service)
	sshConfig, err := sshHandler.GetConfig(ctx, host.ID)
	if err != nil {
		return logicErrf(err, "error getting ssh config")
	}
	nfsServer, err := nfs.NewServer(sshConfig)
	if err != nil {
		return logicErrf(err, "error creating nfs service")
	}
	err = nfsServer.UnmountVGDevice(attachment.Device, volume.Name)
	if err != nil {
		return logicErrf(err, "error unmounting block device")
	}

	// FIXME Delete volume attachments of the children
	for _, volumeSliceInfo := range volume.PVM {
		volumeSlice, err := handler.Get(volumeSliceInfo.Name)
		if err != nil {
			switch err.(type) {
			case resources.ErrResourceNotFound:
				return infraErr(err)
			default:
				return infraErr(resources.ResourceNotFoundError("volume", volumeSliceInfo.Name))
			}
		}

		// Check the volume is effectively attached
		attachment, found := hostVolumesV1.VolumesByID[volumeSlice.ID]
		if !found {
			return logicErr(fmt.Errorf("Can't detach volume '%s': not attached to host '%s'", volumeName, host.Name))
		}

		log.Debugf("Host [%s] : Detaching subVolume with device [%s], attachId [%s]", host.Name, attachment.Device, attachment.AttachID)

		var volumeAttachedV1 *propsv1.VolumeAttachments
		err = volumeSlice.Properties.LockForRead(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
			volumeAttachedV1 = v.(*propsv1.VolumeAttachments)
			return nil
		})
		if err != nil {
			return infraErrf(err, "can't get volume mount status")
		}
		if len(volumeAttachedV1.Hosts) == 0 {
			return logicErr(fmt.Errorf("volume '%s' is already detached", volumeSlice.Name))
		}

		// ... then detach volume
		err = handler.service.DeleteVolumeAttachment(host.ID, attachment.AttachID)
		if err != nil {
			return infraErr(err)
		}

		// update host information
		err = host.Properties.LockForWrite(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
			hostVolumesV1 = v.(*propsv1.HostVolumes)
			// clean host data
			hostVolumesV1.Delete(volumeSlice.ID, volumeSlice.Name, attachment.Device)
			return nil
		})
		if err != nil {
			return infraErr(err)
		}
		err = host.Properties.LockForWrite(HostProperty.MountsV1).ThenUse(func(v interface{}) error {
			hostMountsV1 := v.(*propsv1.HostMounts)

			// FIXME Maybe path requires some tweaking...
			device := attachment.Device
			path := hostMountsV1.LocalMountsByDevice[device]
			mount := hostMountsV1.LocalMountsByPath[path]
			if mount == nil {
				return logicErr(errors.Wrap(fmt.Errorf("metadata inconsistency: no mount corresponding to volume attachment, device [%s], path [%s]", device, path), ""))
			}

			// Updates host property propsv1.MountsV1
			delete(hostMountsV1.LocalMountsByDevice, mount.Device)
			delete(hostMountsV1.LocalMountsByPath, mount.Path)
			return nil
		})
		if err != nil {
			return infraErr(err)
		}

		log.Debugf("Deleting LVM VAT %s", host.ID)
		err = volume.Properties.LockForWrite(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
			volumeAttachedV1 = v.(*propsv1.VolumeAttachments)
			delete(volumeAttachedV1.Hosts, host.ID)
			return nil
		})
		if err != nil {
			return infraErr(err)
		}

		_, err = metadata.SaveVolume(handler.service, volume)
		if err != nil {
			return infraErr(err)
		}

		// Updates volume property propsv1.VolumeAttachments
		log.Debugf("Deleting VAT %s", host.ID)
		err = volumeSlice.Properties.LockForWrite(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
			volumeAttachedV1 = v.(*propsv1.VolumeAttachments)
			delete(volumeAttachedV1.Hosts, host.ID)
			return nil
		})
		if err != nil {
			return infraErr(err)
		}

		_, err = metadata.SaveVolume(handler.service, volumeSlice)
		if err != nil {
			return infraErr(err)
		}
	}

	err = volume.Properties.LockForWrite(VolumeProperty.AttachedV1).ThenUse(func(v interface{}) error {
		volumeAttachedV1 = v.(*propsv1.VolumeAttachments)
		delete(volumeAttachedV1.Hosts, host.ID)
		return nil
	})
	if err != nil {
		return infraErrf(err, "can't attach volume")
	}

	err = host.Properties.LockForWrite(HostProperty.VolumesV1).ThenUse(func(v interface{}) error {
		hostVolumesV1 = v.(*propsv1.HostVolumes)
		// Updates host property propsv1.VolumesV1
		delete(hostVolumesV1.VolumesByID, volume.ID)
		delete(hostVolumesV1.VolumesByName, volume.Name)
		delete(hostVolumesV1.VolumesByDevice, attachment.Device)
		delete(hostVolumesV1.DevicesByID, volume.ID)
		return nil
	})
	if err != nil {
		return infraErrf(err, "can't attach volume")
	}

	// Updates metadata
	_, err = metadata.SaveHost(handler.service, host)
	if err != nil {
		return infraErr(err)
	}

	_, err = metadata.SaveVolume(handler.service, volume)
	if err != nil {
		return infraErr(err)
	}

	return nil
}
