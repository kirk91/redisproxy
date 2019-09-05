package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/kirk91/stats"
	"k8s.io/klog"

	"github.com/kirk91/redisproxy/proxy"
)

var (
	bind     int
	hostsStr string
)

func init() {
	flag.IntVar(&bind, "bind", 6379, "The binding port of proxy")
	flag.StringVar(&hostsStr, "hosts", "", "The hosts of redis, seperated by comma")
	flag.Parse()
}

func main() {
	hosts := strings.Split(hostsStr, ",")
	if len(hosts) == 0 {
		klog.Fatal("empty hosts")
	}

	store := stats.NewStore(stats.NewStoreOption())
	go store.FlushingLoop(context.Background())
	cfg := &proxy.Config{Bind: bind, Hosts: hosts}
	p, err := proxy.New(cfg, store.CreateScope(""))
	if err != nil {
		klog.Fatal(err)
	}
	go func() {
		err := p.ListenAndServe()
		if err != nil {
			klog.Fatal(err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	for s := range c {
		klog.Info("gignal received: ", s)
		switch s {
		case syscall.SIGINT, syscall.SIGTERM: // exit
			p.Stop()
			klog.Info("ready to exit, bye bye...")
			os.Exit(0)
		default:
			klog.V(4).Infof("ignore signal: %s", s)
		}
	}
}
