package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"unicode"

	"github.com/ipfs/go-cid"
	ipfsApi "github.com/ipfs/go-ipfs-api"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	hamtcontainer "github.com/simplecoincom/go-ipld-adl-hamt-container"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/utils"
	"github.com/spf13/cobra"
)

var hostFlag string

var rootCmd = &cobra.Command{
	Use: "hamtcli",
}

var versionCmd = &cobra.Command{
	Use: "version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("hamtcli -- HAMT Container test tool")
	},
}

var setKeyCmd = &cobra.Command{
	Use:   "set",
	Short: "Sets a key value to the HAMT container",
	Args:  cobra.MinimumNArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {
		link := args[0]
		kvs := args[1:]

		if len(kvs)%2 != 0 {
			return fmt.Errorf("Key and values should be pairs")
		}

		store := storage.NewIPFSStorage(ipfsApi.NewShell(hostFlag))

		cid, err := cid.Parse(link)
		if err != nil {
			return err
		}

		// Load HAMT from link
		hamt, err := hamtcontainer.NewHAMTBuilder(
			hamtcontainer.WithStorage(store),
			hamtcontainer.WithLink(cidlink.Link{Cid: cid}),
		).Build()
		if err != nil {
			return err
		}

		if err := hamt.MustBuild(func(hamtSetter hamtcontainer.HAMTSetter) error {
			for i := 0; i < len(kvs); i += 2 {
				if err := hamtSetter.Set([]byte(kvs[i]), []byte(kvs[i+1])); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return err
		}

		lnk, err := hamt.GetLink()
		if err != nil {
			return err
		}

		fmt.Printf("HAMT %s link %s\n", string(hamt.Key()), lnk)

		return nil
	},
}

var getKeyCmd = &cobra.Command{
	Use:   "get",
	Short: "Gets a value from HAMT container by the key",
	Args:  cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		link := args[0]
		key := args[1]

		store := storage.NewIPFSStorage(ipfsApi.NewShell(hostFlag))

		cid, err := cid.Parse(link)
		if err != nil {
			return err
		}

		// Load HAMT from link
		hamt, err := hamtcontainer.NewHAMTBuilder(
			hamtcontainer.WithStorage(store),
			hamtcontainer.WithLink(cidlink.Link{Cid: cid}),
		).Build()
		if err != nil {
			return err
		}

		v, err := hamt.GetAsString([]byte(key))
		if err != nil {
			if errors.Is(err, hamtcontainer.ErrHAMTFailedToGetAsString) {
				v, err := hamt.GetAsLink([]byte(key))
				if err != nil {
					return err
				}
				fmt.Printf("HAMT %s result %s\n", string(hamt.Key()), v)
				return nil
			}
			return err
		}

		fmt.Printf("HAMT %s result %s\n", string(hamt.Key()), v)

		return nil
	},
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}

var listKeysValues = &cobra.Command{
	Use:   "list",
	Short: "List the keys values on HAMT container by the key",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		link := args[0]

		store := storage.NewIPFSStorage(ipfsApi.NewShell(hostFlag))

		cid, err := cid.Parse(link)
		if err != nil {
			return err
		}

		// Load HAMT from link
		hamt, err := hamtcontainer.NewHAMTBuilder(
			hamtcontainer.WithStorage(store),
			hamtcontainer.WithLink(cidlink.Link{Cid: cid}),
		).Build()
		if err != nil {
			return err
		}

		hamt.View(func(key []byte, value interface{}) error {
			if isASCII(string(key)) {
				fmt.Printf("key %s ", string(key))
			} else {
				fmt.Printf("key %s ", hex.EncodeToString(key))
			}

			nodeVal, err := utils.NodeValue(value.(ipld.Node))
			if err != nil {
				return err
			}

			switch val := nodeVal.(type) {
			case ipld.Link:
				fmt.Println("link", val)
			case string:
				fmt.Println("value", string(val))
			case []uint8:
				if isASCII(string(key)) {
					fmt.Println("value", hex.EncodeToString(val))
				} else {
					fmt.Println("value", string(val))
				}
			}

			return nil
		})

		return nil
	},
}

var hamtCmd = &cobra.Command{
	Use:   "hamt",
	Short: "Manage hamt",
}

var newHAMTCmd = &cobra.Command{
	Use:   "new",
	Short: "Creates a new HAMT and return the link",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]

		store := storage.NewIPFSStorage(ipfsApi.NewShell(hostFlag))

		// Create the first HAMT
		hamt, err := hamtcontainer.NewHAMTBuilder(
			hamtcontainer.WithKey([]byte(key)),
			hamtcontainer.WithStorage(store),
		).Build()
		if err != nil {
			return err
		}

		if err := hamt.MustBuild(); err != nil {
			return err
		}

		link, err := hamt.GetLink()
		if err != nil {
			return err
		}

		fmt.Printf("HAMT %s link %s\n", string(hamt.Key()), link)
		return nil
	},
}

var setHAMTLinkCmd = &cobra.Command{
	Use:   "link",
	Short: "Creates nested bucket link",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		link := args[0]
		childLink := args[1]

		store := storage.NewIPFSStorage(ipfsApi.NewShell(hostFlag))

		parentCid, err := cid.Parse(link)
		if err != nil {
			return err
		}

		// Load the parent HAMT from link
		parentHamt, err := hamtcontainer.NewHAMTBuilder(
			hamtcontainer.WithStorage(store),
			hamtcontainer.WithLink(cidlink.Link{Cid: parentCid}),
		).Build()
		if err != nil {
			return err
		}

		childCid, err := cid.Parse(childLink)
		if err != nil {
			return err
		}

		// Load the parent HAMT from link
		childHamt, err := hamtcontainer.NewHAMTBuilder(
			hamtcontainer.WithStorage(store),
			hamtcontainer.WithLink(cidlink.Link{Cid: childCid}),
		).Build()
		if err != nil {
			return err
		}

		if err := parentHamt.MustBuild(func(hamtSetter hamtcontainer.HAMTSetter) error {
			return hamtSetter.Set(childHamt.Key(), childHamt)
		}); err != nil {
			return err
		}

		if err := childHamt.MustBuild(); err != nil {
			return err
		}

		parentHamtLink, err := parentHamt.GetLink()
		if err != nil {
			return err
		}

		fmt.Printf("HAMT %s link %s\n", string(parentHamt.Key()), parentHamtLink)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(setKeyCmd)
	rootCmd.AddCommand(getKeyCmd)
	rootCmd.AddCommand(hamtCmd)
	rootCmd.AddCommand(listKeysValues)

	hamtCmd.AddCommand(setHAMTLinkCmd)
	hamtCmd.AddCommand(newHAMTCmd)
}

func main() {
	rootCmd.PersistentFlags().StringVarP(&hostFlag, "host", "H", "", "host of the IPFS node")

	if len(hostFlag) == 0 {
		tmpHostFlag, ok := os.LookupEnv("IPFS_URL")
		if !ok {
			hostFlag = "http://localhost:5001"
		} else {
			hostFlag = tmpHostFlag
		}
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
