package network

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/adapters"
	"github.com/ethereum/go-ethereum/p2p/protocols"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	p2ptest "github.com/ethereum/go-ethereum/p2p/testing"
)

const (
	protocolName    = "foo"
	protocolVersion = 42
)

func init() {
	h := log.CallerFileHandler(log.StreamHandler(os.Stdout, log.TerminalFormat(true)))
    log.Root().SetHandler(h)
}

// example protocol implementation peer
// message handlers are methods of this
// goal is that we can use the same for "normal" p2p.protocols operations aswell as pss
type pssTestPeer struct {
	*protocols.Peer
	hasProtocol bool
	successC chan bool
	resultC chan int
}

// example node simulation peer
// modeled from swarm/network/simulations/discovery/discovery_test.go - commit 08b1e42f
type pssTestNode struct {
	*Hive
	*Pss
	adapters.NodeAdapter

	id      *adapters.NodeId
	network *simulations.Network
	trigger chan *adapters.NodeId
	run     adapters.ProtoCall
	ct      *protocols.CodeMap
	expectC chan []int
}

func (n *pssTestNode) Add(peer Peer) error {
	err := n.Hive.Add(peer)
	time.Sleep(time.Second)
	n.triggerCheck()
	return err
}

func (n *pssTestNode) Start() error {
	return n.Hive.Start(n.connectPeer, n.hiveKeepAlive)
}

func (n *pssTestNode) Stop() error {
	n.Hive.Stop()
	return nil
}

func (n *pssTestNode) connectPeer(s string) error {
	return n.network.Connect(n.id, adapters.NewNodeIdFromHex(s))
}

func (n *pssTestNode) hiveKeepAlive() <-chan time.Time {
	return time.Tick(time.Second * 10)
}

func (n *pssTestNode) triggerCheck() {
	// TODO: rate limit the trigger?
	go func() { n.trigger <- n.id }()
}

func (n *pssTestNode) RunProtocol(id *adapters.NodeId, rw, rrw p2p.MsgReadWriter, peer *adapters.Peer) error {
	return n.NodeAdapter.(adapters.ProtocolRunner).RunProtocol(id, rw, rrw, peer)
}

func (n *pssTestNode) ProtoCall() adapters.ProtoCall {
	return n.run
}

func (n *pssTestNode) OverlayAddr() []byte {
	return n.Pss.Overlay.GetAddr().OverlayAddr()
}

func (n *pssTestNode) UnderlayAddr() []byte {
	return n.id.Bytes()
}

// the content of the msgs we're sending in the tests
type PssTestPayload struct {
	Data string
}

func (m *PssTestPayload) String() string {
	return m.Data
}

func TestPssCache(t *testing.T) {
	to, _ := hex.DecodeString("08090a0b0c0d0e0f1011121314150001020304050607161718191a1b1c1d1e1f")
	oaddr, _ := hex.DecodeString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	uaddr, _ := hex.DecodeString("101112131415161718191a1b1c1d1e1f000102030405060708090a0b0c0d0e0f")
	waittime := 250 * time.Millisecond
	ps := makePss(oaddr, waittime)
	topic, _ := MakeTopic(protocolName, protocolVersion)
	data := []byte("foo")
	msg := &PssMsg{
		Payload: pssEnvelope{
			TTL:         0,
			SenderOAddr: oaddr,
			SenderUAddr: uaddr,
			Topic:       topic,
			Payload:     data,
		},
	}
	msg.SetRecipient(to)

	msgtwo := &PssMsg{
		Payload: pssEnvelope{
			TTL:         0,
			SenderOAddr: uaddr,
			SenderUAddr: oaddr,
			Topic:       topic,
			Payload:     data,
		},
	}
	msgtwo.SetRecipient(to)

	digest := ps.hashMsg(msg)
	digesttwo := ps.hashMsg(msgtwo)

	if digest != 3595343914 {
		t.Fatalf("digest - got: %d, expected: %d", digest, 3595343914)
	}

	if digest == digesttwo {
		t.Fatalf("different msgs return same crc: %d", digesttwo)
	}

	err := ps.addToFwdCache(digest)
	if err != nil {
		t.Fatalf("write to pss cache failed: %v", err)
	}

	if !ps.checkCache(digest) {
		t.Fatalf("message %v should be in cache but checkCache returned false", msg)
	}

	if ps.checkCache(digesttwo) {
		t.Fatalf("message %v should NOT be in cache but checkCache returned true", msgtwo)
	}

	time.Sleep(waittime)
	if ps.checkCache(digest) {
		t.Fatalf("message %v should have expired from cache but checkCache returned true", msg)
	}
}

func TestPssRegisterHandler(t *testing.T) {
	var topic PssTopic
	var err error
	addr := RandomAddr()
	ps := makePss(addr.UnderlayAddr(), 0)
	
	topic, _ = MakeTopic(protocolName, protocolVersion)
	err = ps.Register(topic, func(msg []byte, p *p2p.Peer, sender []byte) error { return nil })
	if err != nil {
		t.Fatalf("couldnt register protocol 'foo' v 42: %v", err)
	}
	
	topic, _ = MakeTopic(protocolName, protocolVersion)
	err = ps.Register(topic, func(msg []byte, p *p2p.Peer, sender []byte) error { return nil })
	if err == nil {
		t.Fatalf("register protocol 'abc..789' v 65536 should have failed")
	}
}

func TestPssAddSingleHandler(t *testing.T) {

	addr := RandomAddr()

	//ps := newPssBase(t, name, version, addr)
	ps := makePss(addr.OverlayAddr(), 0)
	vct := protocols.NewCodeMap(protocolName, uint(protocolVersion), 65535, &PssTestPayload{})

	// topic will be the mapping in pss used to dispatch to the proper handler
	// the dispatcher is protocol agnostic
	topic, _ := MakeTopic(protocolName, protocolVersion)

	// this is the protocols.Protocol that we want to be made accessible through Pss
	// set up the protocol mapping to pss, and register it for this topic
	// this is an optional step, we are not forcing to use protocols in the handling of pss, it might be anything
	targetprotocol := makeCustomProtocol(protocolName, protocolVersion, vct, nil)
	pssprotocol := NewPssProtocol(ps, &topic, vct, targetprotocol)
	ps.Register(topic, pssprotocol.GetHandler())

	handlefunc := makePssHandleForward(ps)

	newPssProtocolTester(t, ps, addr, 0, handlefunc)
}

func TestPssFullRandom50Pct(t *testing.T) {
var action func(ctx context.Context) error
	var i int
	var check func(ctx context.Context, id *adapters.NodeId) (bool, error)
	var ctx context.Context
	var result *simulations.StepResult
	var timeout time.Duration
	var cancel context.CancelFunc
	
	numsends := 5
	numnodes := 10
	numfullnodes := 5
	fullnodes := []*adapters.NodeId{}
	sends := []int{} // sender/receiver ids array indices pairs
	expectnodes := make(map[*adapters.NodeId]int) // how many messages we're expecting on each respective node
	expectnodesids := []*adapters.NodeId{} // the nodes to expect on (needed by checker)
	expectnodesresults := make(map[*adapters.NodeId][]int) // which messages expect actually got
	
	vct := protocols.NewCodeMap(protocolName, protocolVersion, 65535, &PssTestPayload{})
	topic, _ := MakeTopic(protocolName, protocolVersion)

	trigger := make(chan *adapters.NodeId)
	net := simulations.NewNetwork(&simulations.NetworkConfig{
		Id:      "0",
		Backend: true,
	})
	testpeers := make(map[*adapters.NodeId]*pssTestPeer)
	nodes := newPssSimulationTester(t, numnodes, numfullnodes, net, trigger, vct, protocolName, protocolVersion, testpeers)
	ids := []*adapters.NodeId{}
	
	// connect the peers
	action = func(ctx context.Context) error {
		for id, _ := range nodes {
			ids = append(ids, id)
			if _, ok := testpeers[id]; ok {
				log.Trace(fmt.Sprintf("adding fullnode %x to testpeers %p", common.ByteLabel(id.Bytes()), testpeers))
				fullnodes = append(fullnodes, id)
			}
		}
		for i, id := range ids {
			var peerId *adapters.NodeId
			if i != 0 {
				peerId = ids[i-1]
				if err := net.Connect(id, peerId); err != nil {
					return err
				}
			}
		}
		return nil
	}
	check = func(ctx context.Context, id *adapters.NodeId) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		node, ok := nodes[id]
		if !ok {
			return false, fmt.Errorf("unknown node: %s (%v)", id, node)
		} else {
			log.Trace(fmt.Sprintf("sim check ok node %v", id))
		}

		return true, nil
	}
	
	timeout = 10 * time.Second
	ctx, cancel = context.WithTimeout(context.Background(), timeout)

	result = simulations.NewSimulation(net).Run(ctx, &simulations.Step{
		Action:  action,
		Trigger: trigger,
		Expect: &simulations.Expectation{
			Nodes: ids,
			Check: check,
		},
	})
	if result.Error != nil {
		t.Fatalf("simulation failed: %s", result.Error)
	}
	cancel()

	// ensure that we didn't get lost in concurrency issues
	if len(fullnodes) != numfullnodes {
		t.Fatalf("corrupt fullnodes array, expected %d, have %d", numfullnodes, len(fullnodes))
	}
	
	// ensure that the channel is clean
	trigger = make(chan *adapters.NodeId)

	// randomly decide which nodes to send to and from
	rand.Seed(time.Now().Unix())
	for i = 0; i < numsends; i++ {
		s := rand.Int() % numfullnodes
		r := s
		for ;r == s; {
			r = rand.Int() % numfullnodes
		}
		log.Trace(fmt.Sprintf("rnd pss: idx %d->%d (%x -> %x)", s, r, common.ByteLabel(fullnodes[s].Bytes()), common.ByteLabel(fullnodes[r].Bytes())))
		expectnodes[fullnodes[r]]++
		sends = append(sends, s, r) 
	}
	
	// distinct array of nodes to expect on
	for k, _ := range expectnodes {
		expectnodesids = append(expectnodesids, k)
	}

	// wait a bit for the kademlias to fill up
	time.Sleep(time.Second * 3)
	
	// send and monitor receive of pss
	action = func(ctx context.Context) error {
		code, _ := vct.GetCode(&PssTestPayload{})
		
		for i := 0; i < len(sends); i += 2 {
			msgbytes, _ := makeMsg(code, &PssTestPayload{
				Data: fmt.Sprintf("%v", i + 1),
			})
			go func(i int, expectnodesresults map[*adapters.NodeId][]int) {
				expectnode := fullnodes[sends[i+1]] // the receiving node
				sendnode := fullnodes[sends[i]] // the sending node
				oaddr := nodes[expectnode].OverlayAddr()
				err := nodes[sendnode].Pss.Send(oaddr, topic, msgbytes)
				if err != nil {
					t.Fatalf("could not send pss: %v", err)
				}
				
				select {
					// if the pss is delivered
					case <-testpeers[expectnode].successC:
						log.Trace(fmt.Sprintf("got successC from node %x", common.ByteLabel(expectnode.Bytes())))
						expectnodesresults[expectnode] = append(expectnodesresults[expectnode], <- testpeers[expectnode].resultC)
					// if not we time out, -1 means fail tick
					case <-time.NewTimer(time.Second).C:
						log.Trace(fmt.Sprintf("result timed out on node %x" , common.ByteLabel(expectnode.Bytes())))
						expectnodesresults[expectnode] = append(expectnodesresults[expectnode], -1)
				}
				
				// we can safely send to the check handler if we got feedback for all msgs we sent to a particular node
				if len(expectnodesresults[expectnode]) == expectnodes[expectnode] {
					trigger <- expectnode
					nodes[expectnode].expectC <- expectnodesresults[expectnode]
				}
			}(i, expectnodesresults)
		}
		return nil
	}
	
	// results
	check = func(ctx context.Context, id *adapters.NodeId) (bool, error) {
		select {
			case <-ctx.Done():
				return false, ctx.Err()
			default:
		}
		
		receives := <-nodes[id].expectC
		log.Trace(fmt.Sprintf("expect received %d msgs on from node %x: %v", len(receives), common.ByteLabel(id.Bytes()), receives))
		return true, nil
	}

	timeout = 10 * time.Second
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	
	result = simulations.NewSimulation(net).Run(ctx, &simulations.Step{
		Action:  action,
		Trigger: trigger,
		Expect: &simulations.Expectation{
			Nodes: expectnodesids,
			Check: check,
		},
	})
	if result.Error != nil {
		t.Fatalf("simulation failed: %s", result.Error)
	}

	t.Log("Simulation Passed:")
}

// pss simulation test
// (simnodes running protocols)
func TestPssFullLinearEcho(t *testing.T) {

	var action func(ctx context.Context) error
	var check func(ctx context.Context, id *adapters.NodeId) (bool, error)
	var ctx context.Context
	var result *simulations.StepResult
	var timeout time.Duration
	var cancel context.CancelFunc

	var firstpssnode *adapters.NodeId
	var secondpssnode *adapters.NodeId

	vct := protocols.NewCodeMap(protocolName, protocolVersion, 65535, &PssTestPayload{})
	topic, _ := MakeTopic(protocolName, protocolVersion)
	
	fullnodes := []*adapters.NodeId{}
	trigger := make(chan *adapters.NodeId)
	net := simulations.NewNetwork(&simulations.NetworkConfig{
		Id:      "0",
		Backend: true,
	})
	testpeers := make(map[*adapters.NodeId]*pssTestPeer)
	nodes := newPssSimulationTester(t, 3, 2, net, trigger, vct, protocolName, protocolVersion, testpeers)
	ids := []*adapters.NodeId{} // ohh risky! but the action for a specific id should come before the expect anyway

	action = func(ctx context.Context) error {
		for id, _ := range nodes {
			ids = append(ids, id)
			if _, ok := testpeers[id]; ok {
				log.Trace(fmt.Sprintf("adding fullnode %x to testpeers %p", common.ByteLabel(id.Bytes()), testpeers))
				fullnodes = append(fullnodes, id)
			}
		}
		for i, id := range ids {
			var peerId *adapters.NodeId
			if i != 0 {
				peerId = ids[i-1]
				if err := net.Connect(id, peerId); err != nil {
					return err
				}
			}
		}
		return nil
	}
	check = func(ctx context.Context, id *adapters.NodeId) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		node, ok := nodes[id]
		if !ok {
			return false, fmt.Errorf("unknown node: %s (%v)", id, node)
		} else {
			log.Trace(fmt.Sprintf("sim check ok node %v", id))
		}

		return true, nil
	}

	timeout = 10 * time.Second
	ctx, cancel = context.WithTimeout(context.Background(), timeout)

	result = simulations.NewSimulation(net).Run(ctx, &simulations.Step{
		Action:  action,
		Trigger: trigger,
		Expect: &simulations.Expectation{
			Nodes: ids,
			Check: check,
		},
	})
	if result.Error != nil {
		t.Fatalf("simulation failed: %s", result.Error)
	}
	cancel()

	nonode := &adapters.NodeId{}
	firstpssnode = nonode
	secondpssnode = nonode

	// first find a node that we're connected to
	for firstpssnode == nonode {
		log.Debug(fmt.Sprintf("Waiting for pss relaypeer for %x close to %x ...", common.ByteLabel(nodes[ids[0]].OverlayAddr()), common.ByteLabel(nodes[ids[1]].OverlayAddr())))
		nodes[fullnodes[0]].Pss.Overlay.EachLivePeer(nodes[fullnodes[1]].OverlayAddr(), 256, func(p Peer, po int) bool {
			for _, id := range ids {
				if id.NodeID == p.ID() {
					firstpssnode = id
					log.Debug(fmt.Sprintf("PSS relay found; relaynode %v kademlia %v", common.ByteLabel(id.Bytes()), common.ByteLabel(firstpssnode.Bytes())))
				}
			}
			if firstpssnode == nonode {
				return true
			}
			return false
		})
		if firstpssnode == nonode {
			time.Sleep(time.Millisecond * 100)
		}
	}

	// then find the node it's connected to
	for secondpssnode == nonode {
		log.Debug(fmt.Sprintf("PSS kademlia: Waiting for recipientpeer for %x close to %x ...", common.ByteLabel(nodes[ids[0]].OverlayAddr()), common.ByteLabel(nodes[ids[2]].OverlayAddr())))
		nodes[firstpssnode].Pss.Overlay.EachLivePeer(nodes[fullnodes[1]].OverlayAddr(), 256, func(p Peer, po int) bool {
			for _, id := range ids {
				if id.NodeID == p.ID() && id.NodeID != fullnodes[0].NodeID {
					secondpssnode = id
					log.Debug(fmt.Sprintf("PSS recipient found; relaynode %v kademlia %v", common.ByteLabel(id.Bytes()), common.ByteLabel(secondpssnode.Bytes())))
				}
			}
			if secondpssnode == nonode {
				return true
			}
			return false
		})
		if secondpssnode == nonode {
			time.Sleep(time.Millisecond * 100)
		}
	}

	action = func(ctx context.Context) error {
		code, _ := vct.GetCode(&PssTestPayload{})
		msgbytes, _ := makeMsg(code, &PssTestPayload{
			Data: "ping",
		})

		go func() {
			oaddr := nodes[secondpssnode].OverlayAddr()
			err := nodes[ids[0]].Pss.Send(oaddr, topic, msgbytes)
			if err != nil {
				t.Fatalf("could not send pss: %v", err)
			}
			trigger <- ids[0]
		}()

		return nil
	}
	check = func(ctx context.Context, id *adapters.NodeId) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
		
		// also need to know if the protocolpeer is set up
		time.Sleep(time.Millisecond * 100)
		return <- testpeers[ids[0]].successC, nil
		//return true, nil
	}

	timeout = 10 * time.Second
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result = simulations.NewSimulation(net).Run(ctx, &simulations.Step{
		Action:  action,
		Trigger: trigger,
		Expect: &simulations.Expectation{
			Nodes: []*adapters.NodeId{ids[0]},
			Check: check,
		},
	})
	if result.Error != nil {
		t.Fatalf("simulation failed: %s", result.Error)
	}

	t.Log("Simulation Passed:")
}

func TestPssSimpleSelf(t *testing.T) {
	//var err error
	name := "foo"
	version := 42

	addr := RandomAddr()
	senderaddr := RandomAddr()

	//ps := newPssBase(t, name, version, addr)
	ps := makePss(addr.OverlayAddr(), 0)
	vct := protocols.NewCodeMap(protocolName, protocolVersion, 65535, &PssTestPayload{})

	// topic will be the mapping in pss used to dispatch to the proper handler
	// the dispatcher is protocol agnostic
	topic, _ := MakeTopic(name, version)

	// this is the protocols.Protocol that we want to be made accessible through Pss
	// set up the protocol mapping to pss, and register it for this topic
	// this is an optional step, we are not forcing to use protocols in the handling of pss, it might be anything
	targetprotocol := makeCustomProtocol(name, version, vct, nil)
	pssprotocol := NewPssProtocol(ps, &topic, vct, targetprotocol)
	ps.Register(topic, pssprotocol.GetHandler())

	handlefunc := makePssHandleForward(ps)

	pt, ct := newPssProtocolTester(t, ps, addr, 2, handlefunc)

	// pss msg we will send
	pssmsg := makeFakeMsg(ps, vct, topic, senderaddr, "ping")

	peersmsgcode, found := ct.GetCode(&peersMsg{})
	if !found {
		t.Fatalf("peersMsg not defined")
	}

	subpeersmsgcode, found := ct.GetCode(&subPeersMsg{})
	if !found {
		t.Fatalf("subPeersMsg not defined")
	}

	pssmsgcode, found := ct.GetCode(&PssMsg{})
	if !found {
		t.Fatalf("PssMsg not defined")
	}

	//addr_sim := NewPeerAddrFromNodeId(pt.Ids[1])

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
						Code: subpeersmsgcode,
						Msg:  &subPeersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("subPeersMsg to peer %v fail: %v", id, err)
		}
	}

	for _, id := range pt.Ids {
		err := pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: peersmsgcode,
						Msg:  &peersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
			//	t.Fatalf("peersMsg to peer %v fail: %v", id, err)
		}
	}

	pt.TestExchanges(
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg:  pssmsg,
					Peer: pt.Ids[0],
				},
			},
		},
	)

	// wait till pssmsg is processed
	time.Sleep(time.Second)

}

func TestPssSimpleRelay(t *testing.T) {
	//var err error
	name := "foo"
	version := 42

	addr := RandomAddr()
	senderaddr := RandomAddr()

	//ps := newPssBase(t, name, version, addr)
	ps := makePss(addr.OverlayAddr(), 0)
	vct := protocols.NewCodeMap(name, uint(version), 65535, &PssTestPayload{})

	// topic will be the mapping in pss used to dispatch to the proper handler
	// the dispatcher is protocol agnostic
	topic, _ := MakeTopic(name, version)

	// this is the protocols.Protocol that we want to be made accessible through Pss
	// set up the protocol mapping to pss, and register it for this topic
	// this is an optional step, we are not forcing to use protocols in the handling of pss, it might be anything
	targetprotocol := makeCustomProtocol(name, version, vct, nil)
	pssprotocol := NewPssProtocol(ps, &topic, vct, targetprotocol)
	ps.Register(topic, pssprotocol.GetHandler())

	handlefunc := makePssHandleForward(ps)

	pt, ct := newPssProtocolTester(t, ps, addr, 2, handlefunc)

	// pss msg we will send
	pssmsg := makeFakeMsg(ps, vct, topic, senderaddr, "ping")

	peersmsgcode, found := ct.GetCode(&peersMsg{})
	if !found {
		t.Fatalf("peersMsg not defined")
	}

	subpeersmsgcode, found := ct.GetCode(&subPeersMsg{})
	if !found {
		t.Fatalf("subPeersMsg not defined")
	}

	pssmsgcode, found := ct.GetCode(&PssMsg{})
	if !found {
		t.Fatalf("PssMsg not defined")
	}

	//addr_sim := NewPeerAddrFromNodeId(pt.Ids[1])

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
						Code: subpeersmsgcode,
						Msg:  &subPeersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("subPeersMsg to peer %v fail: %v", id, err)
		}
	}

	for _, id := range pt.Ids {
		err := pt.TestExchanges(
			p2ptest.Exchange{
				Expects: []p2ptest.Expect{
					p2ptest.Expect{
						Code: peersmsgcode,
						Msg:  &peersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
			//	t.Fatalf("peersMsg to peer %v fail: %v", id, err)
		}
	}

	err := pt.TestExchanges(
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg:  pssmsg,
					Peer: pt.Ids[0],
				},
			},

			Expects: []p2ptest.Expect{
				p2ptest.Expect{
					Code:    pssmsgcode,
					Msg:     pssmsg,
					Peer:    pt.Ids[0],
					Timeout: time.Second * 2,
				},
			},
		},
	)

	if err != nil {
		t.Fatalf("PssMsg sending %v to %v (pivot) fail: %v", pt.Ids[0], addr.OverlayAddr(), err)
	}
}

func TestPssProtocolReply(t *testing.T) {
	//var err error
	name := "foo"
	version := 42

	addr := RandomAddr()
	senderaddr := RandomAddr()

	//ps := newPssBase(t, name, version, addr)
	ps := makePss(addr.OverlayAddr(), 0)
	vct := protocols.NewCodeMap(name, uint(version), 65535, &PssTestPayload{})

	// topic is used as a mapping in pss used to dispatch to the proper handler for the pssmsg payload
	// the dispatcher is protocol agnostic

	topic, _ := MakeTopic(name, version)

	// this is the protocols.Protocol that we want to be made accessible through Pss
	// set up the protocol mapping to pss, and register it for this topic
	// this is an optional step, we are not forcing to use protocols in the handling of pss, it might be anything
	targetprotocol := makeCustomProtocol(name, version, vct, nil)
	pssprotocol := NewPssProtocol(ps, &topic, vct, targetprotocol)
	ps.Register(topic, pssprotocol.GetHandler())

	handlefunc := makePssHandleProtocol(ps)

	pt, ct := newPssProtocolTester(t, ps, addr, 2, handlefunc)

	peersmsgcode, found := ct.GetCode(&peersMsg{})
	if !found {
		t.Fatalf("peersMsg not defined")
	}

	subpeersmsgcode, found := ct.GetCode(&subPeersMsg{})
	if !found {
		t.Fatalf("subPeersMsg not defined")
	}

	pssmsgcode, found := ct.GetCode(&PssMsg{})
	if !found {
		t.Fatalf("PssMsg not defined")
	}

	// the pss msg we will send
	pssmsg := makeFakeMsg(ps, vct, topic, senderaddr, "ping")

	//addr_sim := NewPeerAddrFromNodeId(pt.Ids[1])

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
						Code: subpeersmsgcode,
						Msg:  &subPeersMsg{},
						Peer: id,
					},
				},
			},
		)
		if err != nil {
			t.Fatalf("subPeersMsg to peer %v fail: %v", id, err)
		}
	}
	/*
		for _, id := range pt.Ids {
			err := pt.TestExchanges(
				p2ptest.Exchange{
					Expects: []p2ptest.Expect{
						p2ptest.Expect{
							Code: peersmsgcode,
							Msg:  &peersMsg{},
							Peer: id,
						},
					},
				},
			)
			if err != nil {
				//	t.Fatalf("peersMsg to peer %v fail: %v", id, err)
			}
		}
	*/

	// we don't want to expect these messages in the sim from now on
	pt.SetIgnoreCodes(subpeersmsgcode, peersmsgcode)

	err := pt.TestExchanges(
		p2ptest.Exchange{
			Triggers: []p2ptest.Trigger{
				p2ptest.Trigger{
					Code: pssmsgcode,
					Msg:  pssmsg,
					Peer: pt.Ids[0],
				},
			},

			Expects: []p2ptest.Expect{
				p2ptest.Expect{
					Code:    pssmsgcode,
					Msg:     pssmsg,
					Peer:    pt.Ids[0],
					Timeout: time.Second * 2,
				},
			},
		},
	)

	if err != nil {
		t.Fatalf("PssMsg sending %v to %v (pivot) fail: %v", pt.Ids[0], addr.OverlayAddr(), err)
	}
}

// numnodes: how many nodes to create
// pssnodeidx: on which node indices to start the pss
// net: the simulated network
// trigger: hook needed for simulation event reporting
// vct: codemap for virtual protocol
// name: name for virtual protocol (and pss topic)
// version: name for virtual protocol (and pss topic)
// testpeers: pss-specific peers, with hook needed for simulation event reporting

func newPssSimulationTester(t *testing.T, numnodes int, numfullnodes int, net *simulations.Network, trigger chan *adapters.NodeId, vct *protocols.CodeMap, name string, version int, testpeers map[*adapters.NodeId]*pssTestPeer) map[*adapters.NodeId]*pssTestNode {
	topic, _ := MakeTopic(name, version)
	nodes := make(map[*adapters.NodeId]*pssTestNode, numnodes)
	psss := make(map[*adapters.NodeId]*Pss)
	net.SetNaf(func(conf *simulations.NodeConfig) adapters.NodeAdapter {
		var node *pssTestNode
		var handlefunc func(interface{}) error
		addr := NewPeerAddrFromNodeId(conf.Id)
		if testpeers[conf.Id] != nil {
			handlefunc = makePssHandleProtocol(psss[conf.Id])
			log.Trace(fmt.Sprintf("Making full protocol id %x addr %x (testpeers %p)", common.ByteLabel(conf.Id.Bytes()), common.ByteLabel(addr.OverlayAddr()), testpeers))
		} else {
			handlefunc = makePssHandleForward(psss[conf.Id])
		}
		node = newPssTester(t, psss[conf.Id], addr, 0, handlefunc, net, trigger)
		nodes[conf.Id] = node
		return node
	})
	ids := adapters.RandomNodeIds(numnodes)
	i := 0
	for _, id := range ids {
		addr := NewPeerAddrFromNodeId(id)
		psss[id] = makePss(addr.OverlayAddr(), 0)
		if i < numfullnodes {
			tp := &pssTestPeer{
				Peer: &protocols.Peer{
					Peer: &p2p.Peer{},
				},
				successC: make(chan bool),
				resultC: make(chan int),
			}
			testpeers[id] = tp
			targetprotocol := makeCustomProtocol(name, version, vct, testpeers[id])
			pssprotocol := NewPssProtocol(psss[id], &topic, vct, targetprotocol)
			psss[id].Register(topic, pssprotocol.GetHandler())
		}

		net.NewNode(&simulations.NodeConfig{Id: id})
		if err := net.Start(id); err != nil {
			t.Fatalf("error starting node %s: %s", id.Label(), err)
		}
		i++
	}
	
	return nodes
}

func newPssTester(t *testing.T, ps *Pss, addr *peerAddr, numsimnodes int, handlefunc func(interface{}) error, net *simulations.Network, trigger chan *adapters.NodeId) *pssTestNode {

	ct := BzzCodeMap()
	ct.Register(&peersMsg{})
	ct.Register(&getPeersMsg{})
	ct.Register(&subPeersMsg{})
	ct.Register(&PssMsg{})

	// set up the outer protocol
	hp := NewHiveParams()
	hp.CallInterval = 250
	hive := NewHive(hp, ps.Overlay)
	nid := adapters.NewNodeId(addr.UnderlayAddr())

	nodeAdapter := adapters.NewSimNode(nid, net)
	node := &pssTestNode{
		Hive:        hive,
		Pss:         ps,
		NodeAdapter: nodeAdapter,
		id:          nid,
		network:     net,
		trigger:     trigger,
		ct:          ct,
		expectC: make(chan []int),
	}

	srv := func(p Peer) error {
		p.Register(&PssMsg{}, handlefunc)
		node.Add(p)
		p.DisconnectHook(func(err error) {
			hive.Remove(p)
		})
		return nil
	}

	node.run = Bzz(addr.OverlayAddr(), addr.UnderlayAddr(), ct, srv, nil, nil).Run
	nodeAdapter.Run = node.run

	return node
}

func newPssProtocolTester(t *testing.T, ps *Pss, addr *peerAddr, numsimnodes int, handlefunc func(interface{}) error) (*p2ptest.ProtocolTester, *protocols.CodeMap) {
	testnode := newPssTester(t, ps, addr, numsimnodes, handlefunc, nil, nil)
	ptt := p2ptest.NewProtocolTester(t, NodeId(addr), numsimnodes, testnode.ProtoCall())
	return ptt, testnode.ct
}

func makePss(addr []byte, cachettl time.Duration) *Pss {
	kp := NewKadParams()
	kp.MinProxBinSize = 3

	pp := NewPssParams()
	
	overlay := NewKademlia(addr, kp)
	ps := NewPss(overlay, pp)
	overlay.Prune(time.Tick(time.Millisecond * 250))
	return ps
}

func makeCustomProtocol(name string, version int, ct *protocols.CodeMap, testpeer *pssTestPeer) *p2p.Protocol {
	run := func(p *protocols.Peer) error {
		log.Trace(fmt.Sprintf("running pss vprotocol on peer %v", p))
		if testpeer == nil {
			testpeer = &pssTestPeer{}
		}
		testpeer.Peer = p
		p.Register(&PssTestPayload{}, testpeer.SimpleHandlePssPayload)
		err := p.Run()
		return err
	}

	return protocols.NewProtocol(name, uint(version), run, ct, nil, nil)
}

func makeFakeMsg(ps *Pss, ct *protocols.CodeMap, topic PssTopic, senderaddr PeerAddr, content string) PssMsg {
	data := PssTestPayload{}
	code, found := ct.GetCode(&data)
	if !found {
		return PssMsg{}
	}

	data.Data = content

	rlpbundle, err := makeMsg(code, data)
	if err != nil {
		return PssMsg{}
	}

	pssenv := pssEnvelope{
		SenderOAddr: senderaddr.OverlayAddr(),
		SenderUAddr: senderaddr.UnderlayAddr(),
		Topic:       topic,
		TTL:         DefaultTTL,
		Payload:     rlpbundle,
	}
	pssmsg := PssMsg{
		Payload: pssenv,
	}
	pssmsg.SetRecipient(ps.Overlay.GetAddr().OverlayAddr())
	
	return pssmsg
}

func makePssHandleForward(ps *Pss) func(msg interface{}) error {
	// for the simple check it passes on the message if it's not for us
	return func(msg interface{}) error {
		pssmsg := msg.(*PssMsg)

		if ps.isSelfRecipient(pssmsg) {
			log.Trace("pss for us .. yay!")
		} else {
			log.Trace("passing on pss")
			return ps.Forward(pssmsg)
		}
		return nil
	}
}

func makePssHandleProtocol(ps *Pss) func(msg interface{}) error {
	return func(msg interface{}) error {
		pssmsg := msg.(*PssMsg)

		if ps.isSelfRecipient(pssmsg) {
			log.Trace("pss for us ... let's process!")
			env := pssmsg.Payload
			umsg := env.Payload // this will be rlp encrypted
			f := ps.GetHandler(env.Topic)
			if f == nil {
				return fmt.Errorf("No registered handler for topic '%s'", env.Topic)
			}
			nid := adapters.NewNodeId(env.SenderUAddr)
			p := p2p.NewPeer(nid.NodeID, adapters.Name(nid.Bytes()), []p2p.Cap{})
			return f(umsg, p, env.SenderOAddr)
		} else {
			log.Trace("pss was for someone else :'(")
			return ps.Forward(pssmsg)
		}
		return nil
	}
}

// echoes an incoming message
// it comes in through
// Any pointer receiver that has protocols.Peer
func (ptp *pssTestPeer) SimpleHandlePssPayload(msg interface{}) error {
	pmsg := msg.(*PssTestPayload)
	log.Trace(fmt.Sprintf("PssTestPayloadhandler got message %v", pmsg))
	if pmsg.Data == "ping" {
		pmsg.Data = "pong"
		log.Trace(fmt.Sprintf("PssTestPayloadhandler reply %v", pmsg))
		ptp.Send(pmsg)
	} else if pmsg.Data == "pong" {
		ptp.successC <- true
	} else {
		res, err := strconv.Atoi(pmsg.Data)
		if err != nil {
			log.Trace(fmt.Sprintf("PssTestPayloadhandlererr %v", err))
			ptp.successC <- false
		} else {
			log.Trace(fmt.Sprintf("PssTestPayloadhandler sending %d on chan", pmsg))
			ptp.successC <- true
			ptp.resultC <- res
		}
	}

	return nil
}
