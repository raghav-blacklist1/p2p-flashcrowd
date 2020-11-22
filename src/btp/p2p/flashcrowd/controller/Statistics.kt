package btp.p2p.flashcrowd.controller

import btp.p2p.flashcrowd.Simulator
import btp.p2p.flashcrowd.activeNodes
import btp.p2p.flashcrowd.protocols.FlashcrowdProtocol
import btp.p2p.flashcrowd.protocols.SwarmingProtocol
import btp.p2p.flashcrowd.stream.Substream
import btp.p2p.flashcrowd.sumByLong
import btp.p2p.flashcrowd.toInt
import peersim.config.Configuration
import peersim.core.CommonState
import peersim.core.Control
import peersim.core.Network
import java.util.Collections.max
import kotlin.math.ceil
import kotlin.math.log10

class Statistics(prefix: String) : Control {

    private val flashcrowdid: Int = Configuration.getPid("$prefix.$PAR_FLASHCROWD")
    private val swarmingid: Int = Configuration.getPid("$prefix.$PAR_SWARMING")
    private val dhtPid: Int = Configuration.getPid("$prefix.$PAR_DHT")

    private fun isConnected(substream: List<Substream>): Pair<Int, Int> {

        val connectedStreams = substream.filter { (it.parents.size > 0) }
        return Pair((connectedStreams.isNotEmpty()).toInt(), (connectedStreams.size == substream.size).toInt())
    }

    private fun maxhopCount(): Int {
        return ceil(
            log10(Network.size() * (Simulator.bandwidthK1 - 1.0) / Simulator.systemCapacity) / (log10(
                Simulator.bandwidthK1.toDouble()
            ) + 0.001)
        ).toInt() + 1
    }

    private fun streamConnectTime(all: Boolean): Long {
        val condStreamConnected: (Substream) -> Boolean = { it.parents.isNotEmpty() }
        val nodeProts = Network::class.activeNodes()
            .map { (it.getProtocol(flashcrowdid) as FlashcrowdProtocol) }
            .filter {
                if (all) it.substreams.all(condStreamConnected)
                else it.substreams.any(condStreamConnected)
            }
        if (nodeProts.isEmpty()) return 0
        return nodeProts.sumByLong {
            val connectTime = if (all) max(it.substreams.map { it.parents.first().first })
            else it.substreams.first(condStreamConnected).parents.first().first
            connectTime - it.joinTime
        } / nodeProts.size
    }

    override fun execute(): Boolean {
        System.err.println("\n--- Statistics at Time: ${CommonState.getTime()} ---\n")

        System.err.println("Nodes in Overlay: ${Network.size()}")

//      churn rate
        val churnRatio = 1 - ((Network::class.activeNodes().size.toDouble()) / Network.size())
        System.err.println("Churn Ratio: $churnRatio")

//      overlay connection stability
        val allSubStreams = Network::class.activeNodes()
            .map { (it.getProtocol(flashcrowdid) as FlashcrowdProtocol).substreams }
        val connectionStatus = allSubStreams.map { isConnected(it) }
        val connStability = connectionStatus.sumBy { it.first }
        val overStability = connectionStatus.sumBy { it.second }
        System.err.println("Fraction of Connected Peers: " + (connStability.toDouble() / allSubStreams.size))
        System.err.println("Fraction of Stable Peers: " + (overStability.toDouble() / allSubStreams.size))

//      outgoing bandwidth utilisation - flashcrowd

        val outBandwidth =
            allSubStreams.sumByDouble { (it.sumByDouble { it.children.size.toDouble() / Simulator.bandwidthK1 }) / it.size }
        System.err.println("Average Utilized Outgoing Bandwidth: " + (outBandwidth / allSubStreams.size))

//      total bandwidth utilisation
        val allMeshSubStreams = Network::class.activeNodes()
            .map { (it.getProtocol(swarmingid) as SwarmingProtocol) }.filter { it.hasSwarmingStarted }
            .map { it.substreams }
        val totalBandwidth =
            allMeshSubStreams.sumByDouble { (it.sumByDouble { (it.children.size.toDouble() + it.parents.size.toDouble()) / Simulator.bandwidthK2 }) / it.size }
        System.err.println("Average Utilized Total Bandwidth: " + (totalBandwidth / allMeshSubStreams.size))

//      max hop count
        System.err.println("Maximum Hop Count: ${maxhopCount()}")

//      stream connect time
        System.err.println("Avg. First Stream Connect Time: ${streamConnectTime(all = false)}")
        System.err.println("Avg. All Stream Connect Time: ${streamConnectTime(all = true)}")
        System.err.println("\n--- End of Statistics ---\n")
        return false
    }

    companion object {
        private const val PAR_FLASHCROWD = "flashcrowd"
        private const val PAR_SWARMING = "swarm"
        private const val PAR_DHT = "dht"
    }
}