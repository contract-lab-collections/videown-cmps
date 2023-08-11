package cesssc

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	cp2p "github.com/CESSProject/p2p-go"
	cp2p_core "github.com/CESSProject/p2p-go/core"
	"github.com/go-logr/logr"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

type PeerState uint8

const (
	NotConnected PeerState = iota
	Connecting
	Connected
	FailedConnecting
)

var peerStateNames = []string{
	NotConnected:     "NotConnected",
	Connecting:       "Connecting",
	Connected:        "Connected",
	FailedConnecting: "FailedConnecting",
}

func (p PeerState) String() string {
	if uint(p) < uint(len(peerStateNames)) {
		return peerStateNames[uint8(p)]
	}
	return "PeerState:" + strconv.Itoa(int(p))
}

type PeerStatesCount map[PeerState]int

func (t PeerStatesCount) String() string {
	i := 0
	n := len(t)
	b := strings.Builder{}
	b.WriteString("{")
	for k, v := range t {
		b.WriteString(k.String())
		b.WriteString(":")
		b.WriteString(fmt.Sprint(v))
		i++
		if i < n {
			b.WriteString(",")
		}
	}
	b.WriteString("}")
	return b.String()
}

type CessStorageClient struct {
	cp2p_core.P2P
	configedBootAddrs []string
	log               logr.Logger
	peerStates        *sync.Map
}

func New(listenPort uint16, workDir string, configedBootAddrs []string, log logr.Logger) (*CessStorageClient, error) {
	bootAddrs := handleBootNodes(configedBootAddrs, &log)
	if len(bootAddrs) == 0 {
		return nil, errors.New("empty boot address")
	}
	c, err := cp2p.New(
		context.Background(),
		cp2p.ListenPort(int(listenPort)),
		cp2p.Workspace(workDir),
		cp2p.BootPeers(bootAddrs),
		cp2p.ProtocolPrefix("/kldr-testnet"),
	)
	if err != nil {
		return nil, err
	}
	p2pm := CessStorageClient{
		configedBootAddrs: configedBootAddrs,
		log:               log,
		peerStates:        &sync.Map{},
	}
	p2pm.P2P = c
	go func() {
		p2pm.listenDiscoverPeerAddr()
	}()
	return &p2pm, nil
}

func (n *CessStorageClient) listenDiscoverPeerAddr() {
	tickDiscover := time.NewTicker(time.Minute)
	defer tickDiscover.Stop()

	var r1 = rate.Every(time.Second * 3)
	var limit = rate.NewLimiter(r1, 1)

	var r2 = rate.Every(time.Minute)
	var printLimit = rate.NewLimiter(r2, 1)
	n.RouteTableFindPeers(0)

LOOP:
	for {
		select {
		case peer, ok := <-n.GetDiscoveredPeers():
			if !ok {
				break LOOP
			}
			if limit.Allow() {
				tickDiscover.Reset(time.Minute)
			}
			if len(peer.Responses) == 0 {
				break
			}
			for _, pai := range peer.Responses {
				pai.Addrs = onlyPublic(pai.Addrs, &n.log)
				if isRoutable(pai) {
					n.handlePeer(pai)
				}
			}
			if printLimit.Allow() {
				countMap := n.countPeerStates()
				n.log.V(1).Info("peer connect state counts", "count", countMap.String())
			}
		case <-tickDiscover.C:
			countMap := n.countPeerStates()
			_, err := n.RouteTableFindPeers(countMap[Connected] + 20)
			if err != nil {
				n.log.Error(err, "")
			}
		}
	}
	n.log.V(1).Info("peer-discovery loop exit")
}

func (n *CessStorageClient) countPeerStates() PeerStatesCount {
	countMap := make(PeerStatesCount)
	n.peerStates.Range(func(_, v any) bool {
		state, ok := v.(PeerState)
		if ok {
			countMap[state]++
		}
		return true
	})
	return countMap
}

func (n *CessStorageClient) handlePeer(pi *peer.AddrInfo) {
	peerState, _ := n.peerStates.LoadOrStore(pi.ID, NotConnected)
	switch peerState.(PeerState) {
	case Connected:
		return
	case NotConnected:
	case Connecting:
		// n.log.V(2).Info("Skipping node as we're already trying to connect", "peer", pi.ID)
		return
	case FailedConnecting:
		// n.log.V(2).Info("We tried to connect previously but couldn't establish a connection, try again", "peer", pi.ID)
	}

	go func() {
		// n.log.V(2).Info("Connecting to peer", "peer", pi)
		n.peerStates.Store(pi.ID, Connecting)
		if err := n.Connect(n.GetCtxQueryFromCtxCancel(), *pi); err != nil {
			// n.log.Error(err, "Error connecting to peer", "peer", pi)
			n.peerStates.Store(pi.ID, FailedConnecting)
			return
		}
		n.peerStates.Store(pi.ID, Connected)
		// n.log.V(1).Info("Peer connected", "peer", pi)
	}()
}

// Filter out addresses that are local - only allow public ones.
func onlyPublic(addrs []ma.Multiaddr, log *logr.Logger) []ma.Multiaddr {
	routable := []ma.Multiaddr{}
	for _, addr := range addrs {
		if isNil(addr, log) {
			continue
		}
		if manet.IsPublicAddr(addr) {
			routable = append(routable, addr)
		}
	}
	return routable
}

func isRoutable(pi *peer.AddrInfo) bool {
	return len(pi.Addrs) > 0
}

func isNil(addr ma.Multiaddr, log *logr.Logger) bool {
	defer func() {
		if r := recover(); r != nil {
			log.Error(r.(error), "catch panic on check Multiaddr is nil!")
		}
	}()
	if addr == nil {
		return true
	}
	//FIXME: may crash as the 'addr' in debug view error: (unreadable could not resolve interface type)
	return reflect.ValueOf(addr).IsNil()
}

func handleBootNodes(bootAddrs []string, log *logr.Logger) []string {
	result := make([]string, 0)
	for _, v := range bootAddrs {
		bootnodes, err := parseMultiaddr(v)
		if err != nil {
			log.Error(err, "parseMultiaddr()", "addr", v)
			continue
		}
		result = append(result, bootnodes...)
		for _, v := range bootnodes {
			addr, err := ma.NewMultiaddr(v)
			if err != nil {
				log.Error(err, "ma.NewMultiaddr()", "addr", v)
				continue
			}
			addrInfo, err := peer.AddrInfoFromP2pAddr(addr)
			if err != nil {
				log.Error(err, "peer.AddrInfoFromP2pAddr", "addr", addr)
				continue
			}
			log.Info("got bootstrap node", "bootNode", v, "addr", addrInfo)
		}
	}
	return result
}

func parseMultiaddr(addr string) ([]string, error) {
	var result = make([]string, 0)
	var realDns = make([]string, 0)

	multiaddr, err := ma.NewMultiaddr(addr)
	if err == nil {
		_, err = peer.AddrInfoFromP2pAddr(multiaddr)
		if err == nil {
			result = append(result, addr)
			return result, nil
		}
	}

	dnsnames, err := net.LookupTXT(addr)
	if err != nil {
		return result, err
	}

	for _, v := range dnsnames {
		if strings.Contains(v, "ip4") && strings.Contains(v, "tcp") && strings.Count(v, "=") == 1 {
			result = append(result, strings.TrimPrefix(v, "dnsaddr="))
		}
	}

	trims := strings.Split(addr, ".")
	domainname := fmt.Sprintf("%s.%s", trims[len(trims)-2], trims[len(trims)-1])

	for _, v := range dnsnames {
		trims = strings.Split(v, "/")
		for _, vv := range trims {
			if strings.Contains(vv, domainname) {
				realDns = append(realDns, vv)
				break
			}
		}
	}

	for _, v := range realDns {
		dnses, err := net.LookupTXT("_dnsaddr." + v)
		if err != nil {
			continue
		}
		for i := 0; i < len(dnses); i++ {
			if strings.Contains(dnses[i], "ip4") && strings.Contains(dnses[i], "tcp") && strings.Count(dnses[i], "=") == 1 {
				var multiaddr = strings.TrimPrefix(dnses[i], "dnsaddr=")
				result = append(result, multiaddr)
			}
		}
	}

	return result, nil
}
