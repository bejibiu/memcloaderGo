package main

import (
	"appinstalledpb"
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
	"google.golang.org/protobuf/proto"
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
		if len(line) > 0 {
			ch <- line
		}
	}
	close(ch)

}
func createAppInstall(paramList []string) (AppsInstalled, error) {
	if len(paramList) != 5 {
		return AppsInstalled{}, errors.New("params is not apps")
	}
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
	for value := range ch {
		paramList := strings.Split(value, "\t")
		if appsInstalled, err := createAppInstall(paramList); err == nil {

			chAppInstaller <- appsInstalled
		} else {
			fmt.Println(err)
			return
		}
	}
	close(chAppInstaller)
}

func createMessage(app AppsInstalled) memcache.Item {

	ua := &appinstalledpb.UserApps{
		Lat:  &app.lat,
		Lon:  &app.lon,
		Apps: app.apps,
	}

	key := fmt.Sprintf("%s:%s", app.devType, app.devId)
	packed, _ := proto.Marshal(ua)
	return memcache.Item{
		Key:   key,
		Value: packed,
	}
}

func main() {
	ch := make(chan string)
	chAppInstaller := make(chan AppsInstalled)
	clients := make(map[string]*memcache.Client)

	clients["idfa"] = memcache.New("127.0.0.1:5001")
	clients["gaid"] = memcache.New("127.0.0.1:5002")
	clients["adid"] = memcache.New("127.0.0.1:5003")
	clients["dvid"] = memcache.New("127.0.0.1:5004")

	go readfiletochain("sample.tsv", ch)
	go fillChanAppInstaledInstance(ch, chAppInstaller)
	for app := range chAppInstaller {
		if memcClient, ok := clients[app.devType]; ok == true {
			message := createMessage(app)
			memcClient.Set(&message)
			fmt.Println(message)
		}
	}
}
