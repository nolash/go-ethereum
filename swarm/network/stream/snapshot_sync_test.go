// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.
package stream

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/simulations"
	"github.com/ethereum/go-ethereum/p2p/simulations/adapters"
	"github.com/ethereum/go-ethereum/pot"
	"github.com/ethereum/go-ethereum/swarm/network"
	streamTesting "github.com/ethereum/go-ethereum/swarm/network/stream/testing"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

const testMinProxBinSize = 2

var (
	pof = pot.DefaultPof(256)

	startTime time.Time
	ids       []discover.NodeID
	datadirs  map[discover.NodeID]string
	conf      *synctestConfig
	ppmap     map[discover.NodeID]*network.PeerPot

	printed bool
)

type synctestConfig struct {
	addrs            [][]byte
	chunks           []storage.Key
	retrievalMap     map[string]map[string]time.Duration
	idToChunksMap    map[discover.NodeID][]int
	chunksToNodesMap map[string][]int
	idToAddrMap      map[discover.NodeID][]byte
	addrToIdMap      map[string]discover.NodeID
}

func init() {
	//rand.Seed(time.Now().Unix())
	rand.Seed(100)

	initSyncTest()
}

//common_test needs to initialize the test in a init() func
//in order for adapters to register the NewStreamerService;
//this service is dependent on some global variables
//we thus need to initialize first as init() as well.
func initSyncTest() {
	//assign the toAddr func so NewStreamerService can build the addr
	toAddr = func(id discover.NodeID) *network.BzzAddr {
		addr := network.NewAddrFromNodeID(id)
		addr.OAddr[0] = byte(0)
		return addr
	}

	//local stores
	stores = make(map[discover.NodeID]storage.ChunkStore)
	//data directories for each node and store
	datadirs = make(map[discover.NodeID]string)
	//deliveries for each node
	deliveries = make(map[discover.NodeID]*Delivery)

	//channel to wait for peers connected
	waitPeerErrC = make(chan error)
	// peerCount function gives the number of peer connections for a nodeID
	// this is needed for the service run function to wait until
	// each protocol  instance runs and the streamer peers are available
	peerCount = func(id discover.NodeID) int {
		if ids[0] == id || ids[len(ids)-1] == id {
			return 1
		}
		return 2
	}
}

func TestSyncing_1_16(t *testing.T)     { testSyncing(t, 1, 16) }
func TestSyncing_1_32(t *testing.T)     { testSyncing(t, 1, 32) }
func TestSyncing_1_64(t *testing.T)     { testSyncing(t, 1, 64) }
func TestSyncing_1_128(t *testing.T)    { testSyncing(t, 1, 128) }
func TestSyncing_1_256(t *testing.T)    { testSyncing(t, 1, 256) }
func TestSyncing_4_16(t *testing.T)     { testSyncing(t, 4, 16) }
func TestSyncing_4_32(t *testing.T)     { testSyncing(t, 4, 32) }
func TestSyncing_4_64(t *testing.T)     { testSyncing(t, 4, 64) }
func TestSyncing_4_128(t *testing.T)    { testSyncing(t, 4, 128) }
func TestSyncing_4_256(t *testing.T)    { testSyncing(t, 4, 256) }
func TestSyncing_8_16(t *testing.T)     { testSyncing(t, 8, 16) }
func TestSyncing_8_32(t *testing.T)     { testSyncing(t, 8, 32) }
func TestSyncing_8_64(t *testing.T)     { testSyncing(t, 8, 64) }
func TestSyncing_8_128(t *testing.T)    { testSyncing(t, 8, 128) }
func TestSyncing_8_256(t *testing.T)    { testSyncing(t, 8, 256) }
func TestSyncing_32_16(t *testing.T)    { testSyncing(t, 32, 16) }
func TestSyncing_32_32(t *testing.T)    { testSyncing(t, 32, 32) }
func TestSyncing_32_64(t *testing.T)    { testSyncing(t, 32, 64) }
func TestSyncing_32_128(t *testing.T)   { testSyncing(t, 32, 128) }
func TestSyncing_32_256(t *testing.T)   { testSyncing(t, 32, 256) }
func TestSyncing_64_256(t *testing.T)   { testSyncing(t, 64, 256) }
func TestSyncing_128_16(t *testing.T)   { testSyncing(t, 128, 16) }
func TestSyncing_128_32(t *testing.T)   { testSyncing(t, 128, 32) }
func TestSyncing_128_64(t *testing.T)   { testSyncing(t, 128, 64) }
func TestSyncing_128_128(t *testing.T)  { testSyncing(t, 128, 128) }
func TestSyncing_128_256(t *testing.T)  { testSyncing(t, 128, 256) }
func TestSyncing_256_16(t *testing.T)   { testSyncing(t, 256, 16) }
func TestSyncing_256_32(t *testing.T)   { testSyncing(t, 256, 32) }
func TestSyncing_256_64(t *testing.T)   { testSyncing(t, 256, 64) }
func TestSyncing_256_128(t *testing.T)  { testSyncing(t, 256, 128) }
func TestSyncing_256_256(t *testing.T)  { testSyncing(t, 256, 256) }
func TestSyncing_1024_16(t *testing.T)  { testSyncing(t, 1024, 16) }
func TestSyncing_1024_32(t *testing.T)  { testSyncing(t, 1024, 32) }
func TestSyncing_1024_64(t *testing.T)  { testSyncing(t, 1024, 64) }
func TestSyncing_1024_128(t *testing.T) { testSyncing(t, 1024, 128) }
func TestSyncing_1024_256(t *testing.T) { testSyncing(t, 1024, 256) }

// Benchmarks to test the average time it takes for an N-node ring
// to full a healthy kademlia topology
/*
func BenchmarkSyncing_1(b *testing.B)   { benchmarkSyncing(b, 1) }
func BenchmarkSyncing_4(b *testing.B)  { benchmarkSyncing(b, 4) }
func BenchmarkSyncing_8(b *testing.B)  { benchmarkSyncing(b, 8) }
func BenchmarkSyncing_32(b *testing.B)  { benchmarkSyncing(b, 32) }
func BenchmarkSyncing_128(b *testing.B) { benchmarkSyncing(b, 128) }
func BenchmarkSyncing_256(b *testing.B) { benchmarkSyncing(b, 256) }
func BenchmarkSyncing_1024(b *testing.B) { benchmarkSyncing(b, 1024) }

func benchmarkSyncing(b *testing.B, chunkCount int) {
	for i := 0; i < b.N; i++ {
		result, err := testSyncing(b.T, chunkCount)
		if err != nil {
			b.Fatalf("setting up simulation failed", result)
		}
		if result.Error != nil {
			b.Logf("simulation failed: %s", result.Error)
		}
	}
}
*/

func testSyncing(t *testing.T, chunkCount int, nodeCount int) {
	ids = make([]discover.NodeID, nodeCount)
	err := runSyncTest(chunkCount, nodeCount)
	if err != nil {
		t.Fatal(err)
	}
}

/*
The test generates the given number of chunks,
then uploads these to a random node.
Afterwards for every chunk generated, the nearest node addresses
are identified, syncing is started, and finally we verify
that the nodes closer to the chunk addresses actually do have
the chunks in their local stores.

The test loads a snapshot file to construct the swarm network,
assuming that the snapshot file identifies a healthy
kademlia network. The snapshot should have 'streamer' in its service list.

This tests LIVE syncing, as the file is uploaded *after* sync streams have been setup.
For HISTORY syncing a different test is needed.
*/
func runSyncTest(chunkCount int, nodeCount int) error {

	//initialize the test struct
	conf = &synctestConfig{}
	//mapping of nearest node addresses for chunk hashes
	conf.retrievalMap = make(map[string]map[string]time.Duration)
	//map of discover ID to indexes of chunks expected at that ID
	conf.idToChunksMap = make(map[discover.NodeID][]int)
	//map of discover ID to kademlia overlay address
	conf.idToAddrMap = make(map[discover.NodeID][]byte)
	//map of overlay address to discover ID
	conf.addrToIdMap = make(map[string]discover.NodeID)
	//First load the snapshot from the file
	net, err := initNetWithSnapshot(nodeCount)
	if err != nil {
		return err
	}

	//after the test, clean up local stores initialized with createLocalStoreForId
	defer localStoreCleanup()
	defer net.Shutdown()

	//get the nodes of the network
	nodes := net.GetNodes()
	//select one index at random...
	idx := rand.Intn(len(nodes))
	//...and get the the node at that index
	//this is the node selected for upload
	node := nodes[idx]

	log.Info("Initializing test config")
	//iterate over all nodes...
	for c := 0; c < len(nodes); c++ {
		//create an array of discovery node IDs
		ids[c] = nodes[c].ID()
		//get the kademlia overlay address from this ID
		a := network.ToOverlayAddr(ids[c].Bytes())
		//append it to the array of all overlay addresses
		conf.addrs = append(conf.addrs, a)
		//the proximity calculation is on overlay addr,
		//the p2p/simulations check func triggers on discover.NodeID,
		//so we need to know which overlay addr maps to which nodeID
		conf.idToAddrMap[ids[c]] = a
		conf.addrToIdMap[string(a)] = ids[c]
	}
	log.Info("Test config successfully initialized")

	ppmap = network.NewPeerPot(testMinProxBinSize, ids, conf.addrs)
	// channel to signal simulation initialisation with action call complete
	// or node disconnections
	disconnectC := make(chan error)
	quitC := make(chan struct{})

	//define the action to be performed before the test checks: start syncing
	action := func(ctx context.Context) error {
		// need to wait till an aynchronous process registers the peers in streamer.peers
		// that is used by Subscribe
		// the global peerCount function tells how many connections each node has
		// TODO: this is to be reimplemented with peerEvent watcher without global var

		//TODO: VALIDATE THE ASSUMPTION THAT THE FOLLOWING CODE IS NOT NEEDED,
		//AS THE SNAPSHOT CONSTRUCTS ALL CONNECTIONS DURING LOAD, SO WE DON'T NEED TO WAIT HERE?

		/*
			i := 0
			for err := range waitPeerErrC {
				fmt.Println("aaaa")
				if err != nil {
					return fmt.Errorf("error waiting for peers: %s", err)
				}
				i++
				if i == len(ids)-1 {
					break
				}
			}

			// wait for connections
			time.Sleep(5 * time.Second)
		*/

		log.Info("Setting up stream subscription")

		// each node Subscribes to each other's swarmChunkServerStreamName
		for j, id := range ids {
			log.Trace(fmt.Sprintf("subscribe: %d", j))
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			client, err := net.GetNode(id).Client()
			if err != nil {
				return err
			}

			if log.Lvl(*loglevel) == log.LvlDebug {
				//print uploading node kademlia
				if j == idx {
					var kt string
					err := client.CallContext(ctx, &kt, "stream_getKad")
					if err != nil {
						return err
					}

					log.Debug("uploading node kad")
					log.Debug(kt)
				}
			}
			err = streamTesting.WatchDisconnections(id, client, disconnectC, quitC)
			if err != nil {
				return err
			}
			err = client.CallContext(ctx, nil, "stream_startSyncing")
			if err != nil {
				return err
			}
		}

		log.Info("Stream subscriptions successfully requested")
		// wait for subscritpions
		//TODO: Implement a proper sync mechanism so that we don't need to Sleep()
		time.Sleep(10 * time.Second)
		//now upload the chunks to the selected random single node
		conf.chunks, err = uploadFileToSingleNodeStore(node.ID(), chunkCount)
		if err != nil {
			return err
		}
		log.Info(fmt.Sprintf("Uploaded %d chunks to random single node", chunkCount))
		//finally map chunks to the closest addresses
		mapKeysToNodes(conf)

		//		go func() {
		//			startTime = time.Now()
		//			ticker := time.NewTicker(time.Second / 10)
		//			for range ticker.C {
		//				checkChunkIsAtNode(conf)
		//			}
		//		}()

		return nil
	}

	trigger := make(chan discover.NodeID)
	//check defines what will be checked during the test
	check := func(ctx context.Context, id discover.NodeID) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case err := <-disconnectC:
			log.Error("Disconnect event detected", "err", err, "ctx", ctx.Err())
			return false, err
		default:
		}

		log.Trace(fmt.Sprintf("Checking node: %s", id))
		//select the local store for the given node
		lstore := stores[id]
		//if there are more than one chunk, test only succeeds if all expected chunks are found
		allSuccess := true

		//all the chunk indexes which are supposed to be found for this node
		localChunks := conf.idToChunksMap[id]
		//for each expected chunk, check if it is in the local store
		for _, ch := range localChunks {
			//get the real chunk by the index in the index array
			chunk := conf.chunks[ch]
			log.Trace(fmt.Sprintf("node has chunk: %s:", chunk))
			//check if the expected chunk is indeed in the localstore
			if _, err := lstore.Get(chunk); err != nil {
				log.Debug(fmt.Sprintf("Chunk %s NOT found for id %s", chunk, id))
				allSuccess = false
			} else {
				log.Trace(fmt.Sprintf("Chunk %s FOUND for id %s", chunk, id))
			}
		}

		return allSuccess, nil
	}

	timeout := 120 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	//for each tick, run the checks on all nodes
	go func() {
		ticker := time.NewTicker(time.Second * 1)
		for range ticker.C {
			for i := 0; i < len(ids); i++ {
				log.Trace(fmt.Sprintf("triggering step %d, id %s", i, ids[i]))
				trigger <- ids[i]
			}
		}
	}()

	log.Info("Starting simulation run...")
	//run the simulation
	result := simulations.NewSimulation(net).Run(ctx, &simulations.Step{
		Action:  action,
		Trigger: trigger,
		Expect: &simulations.Expectation{
			Nodes: ids,
			Check: check,
		},
	})
	log.Info("no stuck")
	close(quitC)
	if result.Error != nil {
		return result.Error
	}

	log.Info("Simulation terminated")
	return nil
}

func (r *TestRegistry) GetKad(ctx context.Context) string {
	return r.delivery.overlay.String()
}

func (r *TestRegistry) StartSyncing(ctx context.Context) error {
	var err error

	if log.Lvl(*loglevel) == log.LvlDebug {
		//address of registry
		add := r.addr.ID()
		//PeerPot for this node
		pp := ppmap[add]
		//call Healthy RPC
		h := r.delivery.overlay.Healthy(pp)
		//print info
		log.Debug(r.delivery.overlay.String())
		log.Debug(fmt.Sprintf("IS HEALTHY: %t", h.GotNN && h.KnowNN && h.Full))
	}

	var kadDepth int

	r.delivery.overlay.EachConn(nil, 256, func(addr network.OverlayConn, po int, nn bool) bool {
		// TODO: stop or expose by kademlia
		if nn {
			kadDepth = po
		}
		return true
	})

	kad, ok := r.delivery.overlay.(*network.Kademlia)
	if !ok {
		return fmt.Errorf("Not a Kademlia!")
	}

	var startPo int
	var endPo int
	var i int

	//iterate over each bin and solicit needed subscription to bins
	kad.EachBin(r.addr.Over(), pof, 0, func(po, size int, f func(func(val pot.Val, i int) bool) bool) bool {

		//identify begin and start index of the bin(s) we want to subscribe to
		if po < kadDepth {
			//not nn
			endPo = po
			if i > 0 {
				startPo = endPo + 1
			}
		} else if endPo < kadDepth || endPo == 0 {
			if po == 0 && kadDepth == 0 {
				startPo = endPo
			} else {
				startPo = endPo + 1
			}
			endPo = maxPO
		}

		// now iterate and subscribe
		for bin := po - startPo; bin <= endPo; bin++ {

			f(func(val pot.Val, i int) bool {
				a := val.(network.OverlayPeer)
				log.Debug(fmt.Sprintf("Requesting subscription by: registry %s from peer %s for bin: %d", r.addr.ID(), conf.addrToIdMap[string(a.Address())], bin))

				err = r.RequestSubscription(conf.addrToIdMap[string(a.Address())], NewStream("SYNC", []byte{uint8(bin)}, true), &Range{}, Top)
				if err != nil {
					log.Error(fmt.Sprintf("Error in RequestSubsciption! %v", err))
					return false
				}
				return true
			})
		}
		i++
		return true
	})

	return nil
}

func checkChunkIsAtNode(conf *synctestConfig) {
	allOk := true
	for chunk, nodes := range conf.chunksToNodesMap {
		for _, node := range nodes {
			if ok, _ := stores[conf.addrToIdMap[string(conf.addrs[node])]].Get([]byte(chunk)); ok != nil {
				if len(conf.retrievalMap[chunk]) == 0 {
					conf.retrievalMap[chunk] = make(map[string]time.Duration)
				}
				if _, ok := conf.retrievalMap[chunk][string(conf.addrs[node])]; !ok {
					conf.retrievalMap[chunk][string(conf.addrs[node])] = time.Since(startTime)
				}
			}
			if conf.retrievalMap[chunk][string(conf.addrs[node])] == 0 {
				allOk = false
			}
		}
	}
	if allOk && !printed {
		log.Info("All chunks arrived at destination")
		for ch, n := range conf.retrievalMap {
			for a, t := range n {
				log.Info(fmt.Sprintf("Chunk %v at node %s took %v ms", storage.Key([]byte((ch))).String()[:8], conf.addrToIdMap[string(a)].String()[0:8], t.Seconds()*1e3))
			}
		}
	}
	printed = true
}

//map chunk keys to addresses which are responsible
func mapKeysToNodes(conf *synctestConfig) {
	kmap := make(map[string][]int)
	nodemap := make(map[string][]int)
	//build a pot for chunk hashes
	np := pot.NewPot(nil, 0)
	indexmap := make(map[string]int)
	for i, a := range conf.addrs {
		indexmap[string(a)] = i
		np, _, _ = pot.Add(np, a, pof)
	}
	//for each address, run EachNeighbour on the chunk hashes pot to identify closest nodes
	log.Trace(fmt.Sprintf("Generated hash chunk(s): %v", conf.chunks))
	for i := 0; i < len(conf.chunks); i++ {
		pl := 256 //highest possible proximity
		var nns []int
		np.EachNeighbour([]byte(conf.chunks[i]), pof, func(val pot.Val, po int) bool {
			a := val.([]byte)
			if pl < 256 && pl != po {
				return false
			}
			if pl == 256 || pl == po {
				log.Trace(fmt.Sprintf("appending %s", conf.addrToIdMap[string(a)]))
				nns = append(nns, indexmap[string(a)])
				nodemap[string(a)] = append(nodemap[string(a)], i)
			}
			if pl == 256 && len(nns) >= testMinProxBinSize {
				//maxProxBinSize has been reached at this po, so save it
				//we will add all other nodes at the same po
				pl = po
			}
			return true
		})
		kmap[string(conf.chunks[i])] = nns
		//log.Debug(fmt.Sprintf("Length for id %s: %d",ids[i],len(kmap[ids[i]])))
	}
	if log.Lvl(*loglevel) == log.LvlTrace {
		for k, v := range nodemap {
			log.Trace(fmt.Sprintf("Node %s: ", conf.addrToIdMap[k]))
			for _, vv := range v {
				log.Trace(fmt.Sprintf("%v", conf.chunks[vv]))
			}
			log.Trace(fmt.Sprintf("%v", conf.addrToIdMap[k]))
		}
		for k, v := range kmap {
			log.Trace(fmt.Sprintf("Chunk %s: ", k))
			for _, vv := range v {
				log.Trace(fmt.Sprintf("%v", conf.addrToIdMap[string(conf.addrs[vv])]))
			}
		}
	}
	for addr, chunks := range nodemap {
		//this selects which chunks are expected to be found with the given node
		conf.idToChunksMap[conf.addrToIdMap[addr]] = chunks
	}
	log.Debug(fmt.Sprintf("Map of expected chunks by ID: %v", conf.idToChunksMap))
	conf.chunksToNodesMap = kmap
}

//upload a file(chunks) to a single local node store
func uploadFileToSingleNodeStore(id discover.NodeID, chunkCount int) ([]storage.Key, error) {
	log.Debug(fmt.Sprintf("Uploading to node id: %s", id))
	lstore := stores[id]
	size := chunkSize
	dpa := storage.NewDPA(lstore, storage.NewChunkerParams())
	dpa.Start()
	var rootkeys []storage.Key
	for i := 0; i < chunkCount; i++ {
		rk, wait, err := dpa.Store(io.LimitReader(crand.Reader, int64(size)), int64(size))
		wait()
		if err != nil {
			return nil, err
		}
		rootkeys = append(rootkeys, (rk))
	}

	defer dpa.Stop()

	return rootkeys, nil
}

//initialize a network from a snapshot
func initNetWithSnapshot(nodeCount int) (*simulations.Network, error) {

	adapter := "sim"

	var a adapters.NodeAdapter
	//add the streamer service to the node adapter
	//discovery["streamer"] = NewStreamerService

	if adapter == "exec" {
		dirname, err := ioutil.TempDir(".", "")
		if err != nil {
			return nil, err
		}
		a = adapters.NewExecAdapter(dirname)
	} else if adapter == "sock" {
		a = adapters.NewSocketAdapter(services)
	} else if adapter == "tcp" {
		a = adapters.NewTCPAdapter(services)
	} else if adapter == "sim" {
		a = adapters.NewSimAdapter(services)
	}

	log.Info("Setting up Snapshot network")

	net := simulations.NewNetwork(a, &simulations.NetworkConfig{
		ID:             "0",
		DefaultService: "streamer",
	})

	f, err := os.Open(fmt.Sprintf("testing/snapshot_%d.json", nodeCount))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	jsonbyte, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	var snap simulations.Snapshot
	err = json.Unmarshal(jsonbyte, &snap)
	if err != nil {
		return nil, err
	}
	err = net.Load(&snap)
	if err != nil {
		return nil, err
	}
	log.Info("Snapshot loaded")
	return net, nil
}
