package network

import (
	"bytes"
	"testing"
	"time"


	"github.com/ethereum/go-ethereum/logger"
	"github.com/ethereum/go-ethereum/logger/glog"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
)

type pssTester struct {
	*p2ptest.ProtocolTester
	ct *protocols.CodeMap
	*Pss
}

type pssProtocolTester struct {
	*PssProtocol
}

func TestPssProtocolStart(t *testing.T) {
	addr := RandomAddr()
	pt := newPssProtocolTester(t, addr, 2, []byte("foo"), 42)
	
	glog.V(logger.Detail).Infof("made protocoltester %v", pt.Name)
}

func newPssProtocolTester(t *testing.T, addr *peerAddr, n int, topic []byte, version uint) *pssProtocolTester {
	bt := newPssTester(t, addr, n)
	pt := &pssProtocolTester{
		PssProtocol: &PssProtocol{
			Pss: bt.Pss,
			Name: topic,
			Version: version,
		},
	}
	ct := protocols.NewCodeMap(string(topic), version, 65535, nil)
	run := func(p *protocols.Peer) error {
		glog.V(logger.Detail).Infof("inside run with peer %v", p)
		return nil
	}
	pt.NewProtocol(run, ct)
	return pt
}


func TestPssTwoToSelf(t *testing.T) {
	addr := RandomAddr()
	pt := newPssTester(t, addr, 2)
	defer pt.Stop()
	payload := []byte("foo42")

	subpeermsgcode, found := pt.ct.GetCode(&subPeersMsg{})
	if !found {
		t.Fatalf("peerMsg not defined")
	}

	pssmsgcode, found := pt.ct.GetCode(&PssMsg{})
	if !found {
		t.Fatalf("PssMsg not defined")
	}

	hs_pivot := correctBzzHandshake(addr)

	for _, id := range pt.Ids {
		hs_sim := correctBzzHandshake(NewPeerAddrFromNodeId(id))
		<-pt.GetPeer(id).Connc
		err := pt.TestExchanges(bzzHandshakeExchange(hs_pivot, hs_sim, id)...)
		if err != nil {
			t.Fatalf("Handshake fail: %v", err)
		}

		err = pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: subpeermsgcode,
						Msg:  &subPeersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("Subpeersmsg to peer %v fail: %v", id, err)
		}
	}

	err := pt.TestExchanges(
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg: &PssMsg{
						To:   addr.OverlayAddr(),
						Data: payload,
					},
					Peer: pt.Ids[0],
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("PssMsg sending %v to %v (pivot) fail: %v", pt.Ids[0], addr.OverlayAddr(), err)
	}

	alarm := time.NewTimer(1000 * time.Millisecond)
	select {
	case data := <-pt.C:
		if !bytes.Equal(data, payload) {
			t.Fatalf("Data transfer failed, expected: %v, got: %v", payload, data)
		}
	case <-alarm.C:
		t.Fatalf("Pivot receive of PssMsg from %v timeout", pt.Ids[0])
	}
}

func TestPssTwoRelaySelf(t *testing.T) {
	// <-(chan bool)(nil)
	addr := RandomAddr()
	pt := newPssTester(t, addr, 2)
	defer pt.Stop()

	subpeermsgcode, found := pt.ct.GetCode(&subPeersMsg{})
	if !found {
		t.Fatalf("peerMsg not defined")
	}
	pssmsgcode, found := pt.ct.GetCode(&PssMsg{})
	if !found {
		t.Fatalf("PssMsg not defined")
	}

	hs_pivot := correctBzzHandshake(addr)

	for _, id := range pt.Ids {
		hs_sim := correctBzzHandshake(NewPeerAddrFromNodeId(id))
		<-pt.GetPeer(id).Connc
		err := pt.TestExchanges(bzzHandshakeExchange(hs_pivot, hs_sim, id)...)
		if err != nil {
			t.Fatalf("Handshake fail: %v", err)
		}

		err = pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: subpeermsgcode,
						Msg:  &subPeersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("Subpeersmsg to peer %v fail: %v", id, err)
		}
	}

	err := pt.TestExchanges(
		p2ptest.Exchange{
			Expects: []p2ptest.Expect{
				p2ptest.Expect{
					Code: pssmsgcode,
					Msg: &PssMsg{
						To:   pt.Ids[0].Bytes(),
						Data: []byte("foo42"),
					},
					Peer: pt.Ids[0],
				},
			},
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg: &PssMsg{
						To:   pt.Ids[0].Bytes(),
						Data: []byte("foo42"),
					},
					Peer: pt.Ids[1],
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("PssMsg routing from %v to %v fail: %v", pt.Ids[0], pt.Ids[1], err)
	}
}

func newPssTester(t *testing.T, addr *peerAddr, n int) *pssTester {
	return newPssBaseTester(t, addr, n)
}

func newPssBaseTester(t *testing.T, addr *peerAddr, n int) *pssTester {
	ct := BzzCodeMap()
	ct.Register(&PssMsg{})
	ct.Register(&peersMsg{})
	ct.Register(&getPeersMsg{})
	ct.Register(&subPeersMsg{}) // why is this public?

	kp := NewKadParams()
	kp.MinProxBinSize = 3
	to := NewKademlia(addr.OverlayAddr(), kp)
	pp := NewHive(NewHiveParams(), to)
	ps := NewPss(to, addr.OverlayAddr())
	net := simulations.NewNetwork(&simulations.NetworkConfig{})
	naf := func(conf *simulations.NodeConfig) adapters.NodeAdapter {
		na := adapters.NewSimNode(conf.Id, net)
		return na
	}
	net.SetNaf(naf)

	srv := func(p Peer) error {
		p.Register(&PssMsg{}, ps.HandlePssMsg)
		pp.Add(p)
		p.DisconnectHook(func(err error) {
			pp.Remove(p)
		})
		return nil
	}
	protocall := Bzz(addr.OverlayAddr(), addr.UnderlayAddr(), ct, srv, nil, nil).Run

	s := p2ptest.NewProtocolTester(t, NodeId(addr), n, protocall)

	return &pssTester{
		ProtocolTester: s,
		ct:             ct,
		Pss:            ps,
	}

}
