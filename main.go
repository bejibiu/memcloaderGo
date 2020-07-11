package main

import (
	"appinstalledpb"
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

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

const NormalErrRate = 0.01

func reaFileToChain(filename string, ch chan string) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	g, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal("can not open gzip file")
	}
	reader := bufio.NewReader(g)
	numberLine := 0
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
			numberLine++
			if numberLine%1000 == 0 {
				fmt.Printf("Read %v from %v \n", numberLine, filename)
			}
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
	var failed, success float64
	for value := range ch {
		paramList := strings.Split(value, "\t")
		if appsInstalled, err := createAppInstall(paramList); err == nil {

			chAppInstaller <- appsInstalled
			success++
		} else {
			log.Println(err)
			failed++
		}
	}
	if total := success + failed; success > 0 && failed/success+failed >= NormalErrRate {
		log.Printf(
			"Too many invalid records (Total: %d | Error: %d)\n", int(total), int(failed),
		)
		return
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

func sendToMemc(clients map[string]*memcache.Client, chAppInstaller chan AppsInstalled,
	memcacheInsertAttempts int, deleyBetweenAttemt time.Duration, dry bool) {

	for app := range chAppInstaller {
		if memcClient, ok := clients[app.devType]; ok == true {
			message := createMessage(app)
			if dry {
				log.Printf("%s -> %s\n", message.Key, strings.Replace(string(message.Value), "\n", " ", -1))
				continue
			}

			for attempt := 0; attempt < memcacheInsertAttempts; attempt++ {
				err := memcClient.Set(&message)
				if err != nil {
					time.Sleep(deleyBetweenAttemt * time.Second)
					continue
				}
			}

			log.Printf("error connect to Memcached: %s\n", app.devType)
			continue

		}
		log.Printf("error parse type: %s\n", app.devType)
		continue

	}
}

func dotRename(dir, fileName string) error {
	return os.Rename(filepath.Join(dir, fileName), fmt.Sprintf("%v.%v", dir, fileName))
}

func processingFile(file string, clients map[string]*memcache.Client, memcacheInsertAttempts int,
	deleyBetweenAttemt time.Duration, dry bool) {

	ch := make(chan string)
	chAppInstaller := make(chan AppsInstalled)
	log.Printf("Start file %v\n", file)
	go reaFileToChain(file, ch)
	go fillChanAppInstaledInstance(ch, chAppInstaller)
	sendToMemc(clients, chAppInstaller, memcacheInsertAttempts, deleyBetweenAttemt, dry)
}

func main() {
	var pattern, duration string
	var idfa, gaid, adid, dvid string
	var memcacheInsertAttempts int
	var deleyBetweenAttemt time.Duration
	var dry bool

	flag.StringVar(&pattern, "pattern", "/data/appsinstalled/*.tsv.gz", "patter files to procesing")
	flag.IntVar(&memcacheInsertAttempts, "attemts", 3, "attemts to try connect to memcache")
	flag.StringVar(&duration, "deleyBetweenAttemt", "3", "deley between attemt to insert into memcache in sec")

	deleyBetweenAttemt, _ = time.ParseDuration(fmt.Sprintf(duration, "s"))

	flag.StringVar(&idfa, "idfa", "127.0.0.1:33013", "address to idfa memcached storage")
	flag.StringVar(&gaid, "gaid", "127.0.0.1:33014", "address to gaid memcached storage")
	flag.StringVar(&adid, "adid", "127.0.0.1:33015", "address to adid memcached storage")
	flag.StringVar(&dvid, "dvid", "127.0.0.1:33016", "address to dvid memcached storage")

	flag.BoolVar(&dry, "dry", false, "")

	flag.Parse()
	log.Printf("Run with pattert:%v\n\t idfa: %v\n\t gaid: %v\n\t adid: %v\n\t dvid: %v\n", pattern, idfa, gaid, adid, dvid)

	clients := make(map[string]*memcache.Client)

	clients["idfa"] = memcache.New(idfa)
	clients["gaid"] = memcache.New(gaid)
	clients["adid"] = memcache.New(adid)
	clients["dvid"] = memcache.New(dvid)

	if files, err := filepath.Glob(pattern); err == nil {
		for _, file := range files {
			dir, fileName := filepath.Split(file)
			if fileName[0] == '.' {
				log.Printf("Skip '%v'", fileName)
				continue
			}
			processingFile(file, clients, memcacheInsertAttempts, deleyBetweenAttemt, dry)

			if err := dotRename(dir, fileName); err != nil {
				log.Fatal("Can't rename file")
			}

			log.Printf("FIle %v was complited and renamed\n", file)
		}

	}
}
