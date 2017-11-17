package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"

	"bitbucket.org/base2genomicsteam/batchit"
	"bitbucket.org/base2genomicsteam/batchit/ddv"
	"bitbucket.org/base2genomicsteam/batchit/exsmount"
	"bitbucket.org/base2genomicsteam/batchit/logof"
	"bitbucket.org/base2genomicsteam/batchit/submit"
)

type progPair struct {
	help string
	main func()
}

var progs = map[string]progPair{
	"ebsmount":   progPair{"create and mount an EBS volume from an EC2 instance", exsmount.Main},
	"efsmount":   progPair{"mount an EFS drive from an EC2 instance", exsmount.EFSMain},
	"localmount": progPair{"RAID and mount local storage", exsmount.LocalMain},
	"logof":      progPair{"get the log of a given job id", logof.Main},
	"submit":     progPair{"run a batch command", submit.Main},
	"ddv":        progPair{"detach and delete a volume by id", ddv.Main},
}

func printProgs() {

	var wtr io.Writer = os.Stdout

	fmt.Fprintf(wtr, "batchit Version: %s\n\n", batchit.Version)

	wtr.Write([]byte(`batchit is a collection of programs most likely to be of use with AWS batch.
It includes convenience operations so that user-scripts can consist of simple scripts.

`))
	var keys []string
	l := 5
	for k := range progs {
		keys = append(keys, k)
		if len(k) > l {
			l = len(k)
		}
	}
	fmtr := "%-" + strconv.Itoa(l) + "s : %s\n"
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(wtr, fmtr, k, progs[k].help)

	}
	os.Exit(1)

}

func main() {

	if len(os.Args) < 2 {
		printProgs()
	}
	var p progPair
	var ok bool
	if p, ok = progs[os.Args[1]]; !ok {
		printProgs()
	}
	// remove the prog name from the call
	os.Args = append(os.Args[:1], os.Args[2:]...)
	p.main()
}
