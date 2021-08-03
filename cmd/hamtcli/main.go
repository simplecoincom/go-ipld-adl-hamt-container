package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/ipfs/go-cid"
	ipfsApi "github.com/ipfs/go-ipfs-api"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	hamtcontainer "github.com/simplecoincom/go-ipld-adl-hamt-container"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
	"github.com/spf13/cobra"
)

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

		store := storage.NewIPFSStorage(ipfsApi.NewShell("http://localhost:5001"))

		cid, err := cid.Parse(link)
		if err != nil {
			return err
		}

		// Load HAMT from link
		hamt, err := hamtcontainer.NewHAMTBuilder().Storage(store).FromLink(cidlink.Link{Cid: cid}).Build()
		if err != nil {
			return err
		}

		// for i := 0; i < len(kvs); i += 2 {
		// 	err = hamt.Set([]byte(kvs[i]), []byte(kvs[i+1]))
		// 	if err != nil {
		// 		return err
		// 	}
		// }

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

		store := storage.NewIPFSStorage(ipfsApi.NewShell("http://localhost:5001"))

		cid, err := cid.Parse(link)
		if err != nil {
			return err
		}

		// Load HAMT from link
		hamt, err := hamtcontainer.NewHAMTBuilder().Storage(store).FromLink(cidlink.Link{Cid: cid}).Build()
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

		store := storage.NewIPFSStorage(ipfsApi.NewShell("http://localhost:5001"))

		// Create the first HAMT
		hamt, err := hamtcontainer.NewHAMTBuilder().Key([]byte(key)).Storage(store).Build()
		if err != nil {
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
		_ = childLink

		fmt.Println("parent child", args[:])

		store := storage.NewIPFSStorage(ipfsApi.NewShell("http://localhost:5001"))

		parentCid, err := cid.Parse(link)
		if err != nil {
			return err
		}

		// Load the parent HAMT from link
		parentHamt, err := hamtcontainer.NewHAMTBuilder().Storage(store).FromLink(cidlink.Link{Cid: parentCid}).Build()
		if err != nil {
			return err
		}

		// childCid, err := cid.Parse(childLink)
		// if err != nil {
		// 	return err
		// }

		// Load the parent HAMT from link
		// childHamt, err := hamtcontainer.NewHAMTBuilder().Storage(store).FromLink(cidlink.Link{Cid: childCid}).Build()
		// if err != nil {
		// 	return err
		// }

		// Set the child hamt as key on parent hamt
		// err = parentHamt.Set(childHamt.Key(), childHamt)
		// if err != nil {
		// 	return err
		// }

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

	hamtCmd.AddCommand(setHAMTLinkCmd)
	hamtCmd.AddCommand(newHAMTCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
