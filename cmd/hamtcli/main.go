package main

import (
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

		// Create the first HAMT
		hamt, err := hamtcontainer.NewHAMTBuilder().Storage(store).FromLink(cidlink.Link{Cid: cid}).Build()
		if err != nil {
			return err
		}

		for i := 0; i < len(kvs); i += 2 {
			err = hamt.Set([]byte(kvs[i]), []byte(kvs[i+1]))
			if err != nil {
				return err
			}
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

		store := storage.NewIPFSStorage(ipfsApi.NewShell("http://localhost:5001"))

		cid, err := cid.Parse(link)
		if err != nil {
			return err
		}

		// Create the first HAMT
		hamt, err := hamtcontainer.NewHAMTBuilder().Storage(store).FromLink(cidlink.Link{Cid: cid}).Build()
		if err != nil {
			return err
		}

		v, err := hamt.GetAsString([]byte(key))
		if err != nil {
			return err
		}

		fmt.Printf("HAMT %s result %s\n", string(hamt.Key()), v)

		return nil
	},
}

var setHAMTCmd = &cobra.Command{
	Use:   "hamt",
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

func init() {
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(setKeyCmd)
	rootCmd.AddCommand(getKeyCmd)
	rootCmd.AddCommand(setHAMTCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
