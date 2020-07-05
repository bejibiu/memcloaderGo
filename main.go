package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

type AppsInstalled struct {
	devType string
	devId   string
	lat     float64
	lon     float64
	apps    []uint32
}

type PipeLinesApps struct {
	parsed_threads []int
	sender_threads []int
}

const NORMAL_ERR_RATE = 0.01

func readfiletochain(filename string, ch chan string) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				fmt.Println(err)
				return
			}
		}
		ch <- line
	}
	close(ch)

}
func createAppInstall(paramList []string) (AppsInstalled, error) {
	lat, err := strconv.ParseFloat(paramList[2], 64)
	if err != nil {
		return AppsInstalled{}, err
	}

	lot, err := strconv.ParseFloat(paramList[3], 64)
	if err != nil {
		return AppsInstalled{}, err
	}
	var apps []uint32
	for _, rawApp := range strings.Split(paramList[4], ",") {
		app, err := strconv.ParseUint(rawApp, 10, 32)
		if err != nil {
			continue
		}
		apps = append(apps, uint32(app))
	}

	return AppsInstalled{
		paramList[0],
		paramList[1],
		lat,
		lot,
		apps,
	}, nil
}

func fillChanAppInstaledInstance(ch chan string, chAppInstaller chan AppsInstalled) {
	for {
		paramList := strings.Split(<-ch, "\t")
		if appsInstalled, err := createAppInstall(paramList); err == nil {

			chAppInstaller <- appsInstalled
		} else {
			fmt.Println(err)
			return
		}
	}
}

func main() {
	ch := make(chan string)
	chAppInstaller := make(chan AppsInstalled)
	go readfiletochain("sample.tsv", ch)
	go fillChanAppInstaledInstance(ch, chAppInstaller)
	for {
		val, ok := <-ch
		if ok != true {
			break
		}
		fmt.Print(val)
	}
}
