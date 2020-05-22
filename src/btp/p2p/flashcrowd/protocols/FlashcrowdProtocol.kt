package btp.p2p.flashcrowd.protocols

import peersim.config.Configuration
import peersim.core.Node
import peersim.edsim.EDProtocol
import peersim.kademlia.events.ProtocolOperation
import peersim.kademlia.KademliaProtocol
import peersim.kademlia.rpc.ResultFindValueOperation

class FlashcrowdProtocol(val prefix: String) : EDProtocol {
    private var myId: Int = 0
    private val dhtPid: Int = Configuration.getPid("$prefix.$PAR_DHT")

    override fun clone(): Any {
        return FlashcrowdProtocol(prefix)
    }

    override fun processEvent(node: Node, pid: Int, event: Any?) {
        if (myId == 0) myId = pid

//        val rank = node?.getID();
//        println(rank)

        // Let the DHT Protocol do its stuff
//        (node.getProtocol(dhtPid) as KademliaProtocol).processEvent(node, dhtPid, event)
        if (event is ProtocolOperation<*>) {
            println(event)
            println("id: ${event.id}, refId: ${event.refMsgId}")
            val d = event.data
            println(d)
        }
    }

    companion object {
        private const val PAR_DHT = "dht"
    }
}