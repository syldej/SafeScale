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

package commands

import (
	"fmt"

	"github.com/urfave/cli"

	pb "github.com/CS-SI/SafeScale/lib"
	"github.com/CS-SI/SafeScale/lib/client"
	"github.com/CS-SI/SafeScale/lib/server/iaas/resources"
	safescaleutils "github.com/CS-SI/SafeScale/lib/server/utils"
	"github.com/CS-SI/SafeScale/lib/utils"
	clitools "github.com/CS-SI/SafeScale/lib/utils/cli"
)

//VolumeCmd volume command
var VolumeCmd = cli.Command{
	Name:  "volume",
	Usage: "volume COMMAND",
	Subcommands: []cli.Command{
		volumeList,
		volumeInspect,
		volumeDelete,
		volumeCreate,
		volumeAttach,
		volumeDetach,
		volumeExpand,
		volumeShrink,
	},
}

var volumeList = cli.Command{
	Name:    "list",
	Aliases: []string{"ls"},
	Usage:   "List available volumes",
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "all",
			Usage: "List all Volumes on tenant (not only those created by SafeScale)",
		}},
	Action: func(c *cli.Context) error {
		volumes, err := client.New().Volume.List(c.Bool("all"), client.DefaultExecutionTimeout)
		if err != nil {
			return clitools.FailureResponse(clitools.ExitOnRPC(utils.Capitalize(client.DecorateError(err, "list of volumes", false).Error())))
		}
		return clitools.SuccessResponse(volumes.Volumes)
	},
}

var volumeInspect = cli.Command{
	Name:      "inspect",
	Aliases:   []string{"show"},
	Usage:     "Inspect volume",
	ArgsUsage: "<Volume_name|Volume_ID>",
	Action: func(c *cli.Context) error {
		if c.NArg() != 1 {
			_ = cli.ShowSubcommandHelp(c)
			return clitools.FailureResponse(clitools.ExitOnInvalidArgument("Missing mandatory argument <Volume_name|Volume_ID>."))
		}

		volumeInfo, err := client.New().Volume.Inspect(c.Args().First(), client.DefaultExecutionTimeout)
		if err != nil {
			return clitools.FailureResponse(clitools.ExitOnRPC(utils.Capitalize(client.DecorateError(err, "inspection of volume", false).Error())))
		}
		return clitools.SuccessResponse(toDisplaybleVolumeInfo(volumeInfo))
	},
}

var volumeDelete = cli.Command{
	Name:      "delete",
	Aliases:   []string{"rm", "remove"},
	Usage:     "Delete volume",
	ArgsUsage: "<Volume_name|Volume_ID> [<Volume_name|Volume_ID>...]",
	Action: func(c *cli.Context) error {
		if c.NArg() < 1 {
			_ = cli.ShowSubcommandHelp(c)
			return clitools.FailureResponse(clitools.ExitOnInvalidArgument("Missing mandatory argument <Volume_name|Volume_ID>."))
		}

		var volumeList []string
		volumeList = append(volumeList, c.Args().First())
		volumeList = append(volumeList, c.Args().Tail()...)

		err := client.New().Volume.Delete(volumeList, client.DefaultExecutionTimeout)
		if err != nil {
			return clitools.FailureResponse(clitools.ExitOnRPC(utils.Capitalize(client.DecorateError(err, "deletion of volume", false).Error())))
		}
		return clitools.SuccessResponse(nil)
	},
}

var volumeCreate = cli.Command{
	Name:      "create",
	Aliases:   []string{"new"},
	Usage:     "Create a volume",
	ArgsUsage: "<Volume_name>",
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "size",
			Value: 10,
			Usage: "Size of the volume (in Go)",
		},
		cli.StringFlag{
			Name:  "speed",
			Value: "HDD",
			Usage: fmt.Sprintf("Allowed values: %s", getAllowedSpeeds()),
		},
		cli.BoolFlag{
			Name:  "resizable",
			Usage: "Use volumes managed by LVM with a granularity defined by VUSize",
		},
		cli.IntFlag{
			Name:  "vusize",
			Value: 4,
			Usage: "Size of each LVM Physical Drive (in Go)",
		},
	},
	Action: func(c *cli.Context) error {
		if c.NArg() != 1 {
			_ = cli.ShowSubcommandHelp(c)
			return clitools.FailureResponse(clitools.ExitOnInvalidArgument("Missing mandatory argument <Volume_name>. "))
		}

		speed := c.String("speed")
		volSpeed, ok := pb.VolumeSpeed_value[speed]
		if !ok {
			return clitools.FailureResponse(clitools.ExitOnInvalidOption(fmt.Sprintf("Invalid speed '%s'", speed)))
		}
		volSize := int32(c.Int("size"))
		if volSize <= 0 {
			return clitools.FailureResponse(clitools.ExitOnInvalidOption(fmt.Sprintf("Invalid volume size '%d', should be at least 1", volSize)))
		}
		def := pb.VolumeDefinition{
			Name:   c.Args().First(),
			Size:   int32(c.Int("size")),
			Speed:  pb.VolumeSpeed(volSpeed),
			InLVM:  c.Bool("resizable"),
			VUSize: int32(c.Int("vusize")),
		}

		volume, err := client.New().Volume.Create(def, client.DefaultExecutionTimeout)
		if err != nil {
			return clitools.FailureResponse(clitools.ExitOnRPC(utils.Capitalize(client.DecorateError(err, "creation of volume", true).Error())))
		}
		return clitools.SuccessResponse(toDisplaybleVolume(volume))
	},
}

var volumeAttach = cli.Command{
	Name:      "attach",
	Usage:     "Attach a volume to an host",
	ArgsUsage: "<Volume_name|Volume_ID> <Host_name|Host_ID>",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "path",
			Value: resources.DefaultVolumeMountPoint,
			Usage: "Mount point of the volume",
		},
		cli.StringFlag{
			Name:  "format",
			Value: "ext4",
			Usage: "Filesystem format",
		},
		cli.BoolFlag{
			Name:  "do-not-format",
			Usage: "Prevent the volume to be formatted (the previous format of the disk will be kept, beware that a new volume has no format before his first attachment and so can't be attach with this option)",
		},
	},
	Action: func(c *cli.Context) error {
		if c.NArg() != 2 {
			_ = cli.ShowSubcommandHelp(c)
			return clitools.FailureResponse(clitools.ExitOnInvalidArgument("Missing mandatory argument <Volume_name> and/or <Host_name>."))
		}
		def := pb.VolumeAttachment{
			Format:      c.String("format"),
			DoNotFormat: c.Bool("do-not-format"),
			MountPath:   c.String("path"),
			Host:        &pb.Reference{Name: c.Args().Get(1)},
			Volume:      &pb.Reference{Name: c.Args().Get(0)},
		}
		err := client.New().Volume.Attach(def, client.DefaultExecutionTimeout)
		if err != nil {
			return clitools.FailureResponse(clitools.ExitOnRPC(utils.Capitalize(client.DecorateError(err, "attach of volume", true).Error())))
		}
		return clitools.SuccessResponse(nil)
	},
}

// splitter

var volumeExpand = cli.Command{
	Name:      "expand",
	Usage:     "Expands a volume",
	ArgsUsage: "<Volume_name|Volume_ID> <Host_name|Host_ID>",
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "uv",
			Value: 0,
			Usage: "Adds volumes of size Unit Volume",
		},
		cli.IntFlag{
			Name:  "gb",
			Value: 0,
			Usage: "Adds Gb to VG",
		},
		cli.IntFlag{
			Name:  "ratio",
			Value: 0,
			Usage: "Adds in %",
		},
	},
	Action: func(c *cli.Context) error {
		if c.NArg() != 2 {
			_ = cli.ShowSubcommandHelp(c)
			return clitools.FailureResponse(clitools.ExitOnInvalidArgument("Missing mandatory argument <Volume_name> and/or <Host_name>"))
		}

		def := pb.VolumeSizeChange{
			PositiveChange: true,
			VolumeName:     &pb.Reference{Name: c.Args().Get(0)},
			HostName:       &pb.Reference{Name: c.Args().Get(1)},
		}

		def.ChangeSizeType = ""

		if c.Uint("gb") > 0 {
			def.ChangeSize = uint32(c.Uint("gb"))
			def.ChangeSizeType = "gb"
		}

		if c.Uint("uv") > 0 {
			if def.ChangeSizeType != "" {
				return clitools.FailureResponse(clitools.ExitOnInvalidArgument("Flags (gb, uv, ratio) are mutually exclusive, only one can be specified at a time"))
			}

			def.ChangeSize = uint32(c.Uint("uv"))
			def.ChangeSizeType = "uv"
		}

		if c.Uint("ratio") > 0 {
			if def.ChangeSizeType != "" {
				return clitools.FailureResponse(clitools.ExitOnInvalidArgument("Flags (gb, uv, ratio) are mutually exclusive, only one can be specified at a time"))
			}

			def.ChangeSize = uint32(c.Uint("ratio"))
			def.ChangeSizeType = "ratio"
		}

		if def.ChangeSizeType == "" {
			return clitools.FailureResponse(clitools.ExitOnInvalidArgument("One flag required (gb, ub, ratio)"))
		}

		err := client.New().Volume.Expand(def, client.DefaultExecutionTimeout)
		if err != nil {
			return clitools.ExitOnRPC(utils.Capitalize(client.DecorateError(err, "attach of volume", true).Error()))
		}
		fmt.Printf("Volume '%s' expanded in host '%s'\n", c.Args().Get(0), c.Args().Get(1))
		return nil
	},
}

var volumeShrink = cli.Command{
	Name:      "shrink",
	Usage:     "Shrinks a volume",
	ArgsUsage: "<Volume_name|Volume_ID> <Host_name|Host_ID>",
	Flags: []cli.Flag{
		cli.IntFlag{
			Name:  "uv",
			Value: 0,
			Usage: "Removes volumes of size Unit Volume",
		},
		cli.IntFlag{
			Name:  "gb",
			Value: 0,
			Usage: "Reduces Gb from VG",
		},
		cli.IntFlag{
			Name:  "ratio",
			Value: 0,
			Usage: "Reduction in %",
		},
		cli.BoolFlag{
			Name:  "auto",
			Usage: "Resizes volume group to a minimum",
		},
	},
	Action: func(c *cli.Context) error {
		if c.NArg() != 2 {
			_ = cli.ShowSubcommandHelp(c)
			return clitools.FailureResponse(clitools.ExitOnInvalidArgument("Missing mandatory argument <Volume_name> and/or <Host_name>"))
		}

		def := pb.VolumeSizeChange{
			PositiveChange: false,
			VolumeName:     &pb.Reference{Name: c.Args().Get(0)},
			HostName:       &pb.Reference{Name: c.Args().Get(1)},
		}

		def.ChangeSizeType = ""

		if c.Uint("gb") > 0 {
			def.ChangeSize = uint32(c.Uint("gb"))
			def.ChangeSizeType = "gb"
		}

		if c.Uint("uv") > 0 {
			if def.ChangeSizeType != "" {
				return clitools.FailureResponse(clitools.ExitOnInvalidArgument("Flags (gb, uv, ratio) are mutually exclusive, only one can be specified at a time"))
			}

			def.ChangeSize = uint32(c.Uint("uv"))
			def.ChangeSizeType = "uv"
		}

		if c.Uint("ratio") > 0 {
			if def.ChangeSizeType != "" {
				return clitools.FailureResponse(clitools.ExitOnInvalidArgument("Flags (gb, uv, ratio) are mutually exclusive, only one can be specified at a time"))
			}

			def.ChangeSize = uint32(c.Uint("ratio"))
			def.ChangeSizeType = "ratio"

			if def.ChangeSize > 100 {
				_ = cli.ShowSubcommandHelp(c)
				return clitools.FailureResponse(clitools.ExitOnInvalidArgument("Invalid ratio"))
			}
		}

		err := client.New().Volume.Shrink(def, client.DefaultExecutionTimeout)
		if err != nil {
			return clitools.ExitOnRPC(utils.Capitalize(client.DecorateError(err, "attach of volume", true).Error()))
		}
		fmt.Printf("Volume '%s' shrinked in host '%s'\n", c.Args().Get(0), c.Args().Get(1))
		return nil
	},
}
var volumeDetach = cli.Command{
	Name:      "detach",
	Usage:     "Detach a volume from an host",
	ArgsUsage: "<Volume_name|Volume_ID> <Host_name|Host_ID>",
	Action: func(c *cli.Context) error {
		if c.NArg() != 2 {
			_ = cli.ShowSubcommandHelp(c)
			return clitools.FailureResponse(clitools.ExitOnInvalidArgument("Missing mandatory argument <Volume_name> and/or <Host_name>."))
		}

		err := client.New().Volume.Detach(c.Args().Get(0), c.Args().Get(1), client.DefaultExecutionTimeout)
		if err != nil {
			return clitools.FailureResponse(clitools.ExitOnRPC(utils.Capitalize(client.DecorateError(err, "unattach of volume", true).Error())))
		}
		return clitools.SuccessResponse(nil)
	},
}

type volumeInfoDisplayable struct {
	ID        string
	Name      string
	Speed     string
	Size      int32
	Host      string
	MountPath string
	Format    string
	Device    string
	InLVM     bool
	PVS       []*volumeInfoDisplayable
}

type volumeDisplayable struct {
	ID    string
	Name  string
	Speed string
	Size  int32
}

func toDisplaybleVolumeInfo(volumeInfo *pb.VolumeInfo) *volumeInfoDisplayable {
	if len(volumeInfo.PVS) == 0 {
		return &volumeInfoDisplayable{
			volumeInfo.GetId(),
			volumeInfo.GetName(),
			pb.VolumeSpeed_name[int32(volumeInfo.GetSpeed())],
			volumeInfo.GetSize(),
			safescaleutils.GetReference(volumeInfo.GetHost()),
			volumeInfo.GetMountPath(),
			volumeInfo.GetFormat(),
			volumeInfo.GetDevice(),
			volumeInfo.GetInLVM(),
			[]*volumeInfoDisplayable{},
		}
	} else {
		var vInfos []*volumeInfoDisplayable

		for _, vin := range volumeInfo.GetPVS() {
			vInfos = append(vInfos, toDisplaybleVolumeInfo(vin))
		}

		return &volumeInfoDisplayable{
			volumeInfo.GetId(),
			volumeInfo.GetName(),
			pb.VolumeSpeed_name[int32(volumeInfo.GetSpeed())],
			volumeInfo.GetSize(),
			safescaleutils.GetReference(volumeInfo.GetHost()),
			volumeInfo.GetMountPath(),
			volumeInfo.GetFormat(),
			volumeInfo.GetDevice(),
			volumeInfo.GetInLVM(),
			vInfos,
		}
	}
}

func toDisplaybleVolume(volumeInfo *pb.Volume) *volumeDisplayable {
	return &volumeDisplayable{
		volumeInfo.GetId(),
		volumeInfo.GetName(),
		pb.VolumeSpeed_name[int32(volumeInfo.GetSpeed())],
		volumeInfo.GetSize(),
	}
}

func getAllowedSpeeds() string {
	speeds := ""
	i := 0
	for k := range pb.VolumeSpeed_value {
		if i > 0 {
			speeds += ", "
		}
		speeds += k
		i++

	}
	return speeds
}
