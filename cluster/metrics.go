/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 */

package cluster

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// --- 9.1 Replication Latency ---

var (
	lslMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_lsl_ms",
		Help: "Light-speed latency to each peer in ms (minimum observed HLC delta over sliding window)",
	}, []string{"peer"})

	protocolOverheadMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_protocol_overhead_ms",
		Help: "Extra latency beyond LSL (observed_latency - lsl)",
	}, []string{"peer"})
)

// --- 9.2 Causality ---

var (
	causalityViolationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cluster_causality_violations_total",
		Help: "Counter of effects arriving with HLC beyond the causal horizon",
	}, []string{"peer"})

	causalHorizonMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_causal_horizon_ms",
		Help: "Causal horizon width per peer: max observed HLC drift (ms). Effects with HLC > now + this value are violations.",
	}, []string{"peer"})

	maxHlcDriftMs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_max_hlc_drift_ms",
		Help: "Maximum observed HLC drift per peer",
	}, []string{"peer"})
)

// --- 9.3 Throughput ---

var (
	WritesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_writes_total",
		Help: "Total local write effects emitted",
	})

	ReadsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_reads_total",
		Help: "Total reads served",
	})

	ReadsLocalTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_reads_local_total",
		Help: "Reads served from local log/cache (no fetch)",
	})

	ReadsRemoteFetchTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_reads_remote_fetch_total",
		Help: "Reads that required a remote fetch",
	})

	NotificationsSentTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_notifications_sent_total",
		Help: "OffsetNotify messages broadcast",
	})

	NotificationsReceivedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_notifications_received_total",
		Help: "OffsetNotify messages received",
	})

	FetchesServedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_fetches_served_total",
		Help: "Fetch RPCs served to peers",
	})

	BindsEmittedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_binds_emitted_total",
		Help: "Bind effects emitted (concurrent writes detected)",
	})

	SnapshotsEmittedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_snapshots_emitted_total",
		Help: "Snapshot effects emitted (bind resolution)",
	})
)

// --- 9.4 Disk ---

var (
	SegmentActiveBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_segment_active_bytes",
		Help: "Bytes used in the current live segment",
	})

	SegmentActiveSlots = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_segment_active_slots",
		Help: "Slots used in the current live segment (out of 1M)",
	})

	SegmentsSealedTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_segments_sealed_total",
		Help: "Number of sealed segments on this node",
	})

	DiskUsedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_disk_used_bytes",
		Help: "Total disk used by all log segments",
	})

	DiskCapacityBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_disk_capacity_bytes",
		Help: "Total disk capacity",
	})

	DiskUsageRatio = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cluster_disk_usage_ratio",
		Help: "Disk usage ratio (used / capacity)",
	})
)

// --- 9.5 Peer Health ---

var (
	peerConnected = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_peer_connected",
		Help: "1 if stream is up, 0 if down",
	}, []string{"peer"})

	peerReconnectsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cluster_peer_reconnects_total",
		Help: "Number of reconnections",
	}, []string{"peer"})

	peerNotificationsDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cluster_peer_notifications_dropped_total",
		Help: "Notifications dropped due to buffer full or disconnected peer",
	}, []string{"peer"})
)

// --- 9.6 Heartbeat & UDP Fast Path ---

var (
	heartbeatsSentTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_heartbeats_sent_total",
		Help: "Total heartbeat packets sent",
	})

	heartbeatsReceivedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_heartbeats_received_total",
		Help: "Total heartbeat packets received",
	})

	peerSymmetricGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_peer_symmetric",
		Help: "1 if peer path is symmetric, 0 if asymmetric",
	}, []string{"peer"})

	peerAliveGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_peer_alive",
		Help: "1 if peer is alive (heartbeat within timeout), 0 if dead",
	}, []string{"peer"})

	peerRttMsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cluster_peer_rtt_ms",
		Help: "Estimated RTT to peer in milliseconds (from heartbeat)",
	}, []string{"peer"})

	udpNotifyAckLatencyMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "cluster_udp_notify_ack_latency_ms",
		Help:    "Latency of UDP notification ACKs in milliseconds",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100},
	})
)

// RecordHeartbeatsSent increments the heartbeat sent counter.
func RecordHeartbeatsSent() {
	heartbeatsSentTotal.Inc()
}

// RecordHeartbeatReceived increments the heartbeat received counter.
func RecordHeartbeatReceived() {
	heartbeatsReceivedTotal.Inc()
}

// RecordPeerSymmetric updates the symmetry gauge for a peer.
func RecordPeerSymmetric(peerID NodeId, symmetric bool) {
	v := float64(0)
	if symmetric {
		v = 1
	}
	peerSymmetricGauge.WithLabelValues(peerLabel(peerID)).Set(v)
}

// RecordPeerAlive updates the alive gauge for a peer.
func RecordPeerAlive(peerID NodeId, alive bool) {
	v := float64(0)
	if alive {
		v = 1
	}
	peerAliveGauge.WithLabelValues(peerLabel(peerID)).Set(v)
}

// RecordPeerRTT updates the RTT gauge for a peer.
func RecordPeerRTT(peerID NodeId, rttNanos int64) {
	peerRttMsGauge.WithLabelValues(peerLabel(peerID)).Set(float64(rttNanos) / 1e6)
}

// RecordUDPNotifyACKLatency records the latency of a notification ACK.
func RecordUDPNotifyACKLatency(latencyMs float64) {
	udpNotifyAckLatencyMs.Observe(latencyMs)
}

// --- 9.7 Retransmission ---

var (
	retransmissionGiveUpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cluster_retransmission_giveups_total",
		Help: "Total retransmission give-ups (max retries exhausted) per peer",
	}, []string{"peer"})
)

// RecordRetransmissionGiveUp increments the give-up counter for a peer.
func RecordRetransmissionGiveUp(peerID NodeId) {
	retransmissionGiveUpsTotal.WithLabelValues(peerLabel(peerID)).Inc()
}

// --- LSL Tracker ---

// Peak-hold decay constants (like audio meter red-lines)
const (
	peakHoldDuration  = 2 * time.Minute  // Hold peak for this long before decay
	peakDecayHalfLife = 30 * time.Second // Half-life for exponential decay
)

// peakHold tracks a peak value with decay behavior like audio meters.
// The peak jumps instantly to new highs, holds for a period, then
// exponentially decays toward the current observed value.
type peakHold struct {
	peak     float64   // Current peak value
	peakTime time.Time // When peak was last set
	current  float64   // Most recent observed value
}

// lslTracker maintains a sliding window minimum for light-speed latency per peer
// and tracks HLC drift with peak-hold decay behavior.
type lslTracker struct {
	mu        sync.Mutex
	minByMs   map[string]float64   // peer label -> min LSL in ms
	driftPeak map[string]*peakHold // peer label -> peak drift with decay
}

var lslState = &lslTracker{
	minByMs:   make(map[string]float64),
	driftPeak: make(map[string]*peakHold),
}

// getPeakWithDecay returns the current peak value after applying decay.
// If the peak was set more than peakHoldDuration ago, it decays exponentially
// toward the current observed value.
func (ph *peakHold) getPeakWithDecay(now time.Time) float64 {
	elapsed := now.Sub(ph.peakTime)
	if elapsed <= peakHoldDuration {
		// Still in hold period, return full peak
		return ph.peak
	}

	// Apply exponential decay toward current value
	decayTime := elapsed - peakHoldDuration
	// decay = e^(-t * ln(2) / halfLife) = 0.5^(t/halfLife)
	decayFactor := math.Pow(0.5, float64(decayTime)/float64(peakDecayHalfLife))

	// Interpolate between current and peak based on decay
	return ph.current + (ph.peak-ph.current)*decayFactor
}

// --- Inline metric update functions ---

// RecordNotificationSent increments the sent counter for a broadcast.
func RecordNotificationSent() {
	NotificationsSentTotal.Inc()
}

// RecordNotificationReceived records receipt of an OffsetNotify from a peer.
// Computes latency metrics from the send_time timestamp (wall clock at send).
// The sendTimeNs parameter is the sender's wall clock at transmission time (nanoseconds).
// If sendTimeNs is 0 (legacy message), falls back to hlcNs for backward compatibility.
func RecordNotificationReceived(peerID NodeId, hlc time.Time, sendTimeNs uint64) {
	NotificationsReceivedTotal.Inc()

	peer := peerLabel(peerID)
	now := time.Now()
	nowMs := float64(now.UnixMilli())

	// Use send_time for drift measurement (actual clock drift between nodes)
	// Fall back to HLC if send_time not present (backward compatibility)
	var driftSourceMs float64
	if sendTimeNs > 0 {
		driftSourceMs = float64(sendTimeNs) / 1e6
	} else {
		driftSourceMs = float64(hlc.UnixNano()) / 1e6
	}

	// Clock drift: sender_wall_clock - receiver_wall_clock
	// Positive = sender clock ahead, Negative = sender clock behind
	driftMs := driftSourceMs - nowMs

	// LSL approximation: minimum observed |drift| over time
	absDrift := math.Abs(driftMs)
	lslState.mu.Lock()
	currentMin, ok := lslState.minByMs[peer]
	if !ok || absDrift < currentMin {
		lslState.minByMs[peer] = absDrift
		currentMin = absDrift
	}

	// Peak-hold drift tracking with decay (like audio meter red-lines)
	ph, exists := lslState.driftPeak[peer]
	var prevPeak float64
	if !exists {
		ph = &peakHold{
			peak:     absDrift,
			peakTime: now,
			current:  absDrift,
		}
		lslState.driftPeak[peer] = ph
		prevPeak = 0 // No previous peak for new peers
	} else {
		// Capture previous peak BEFORE any updates (for violation check)
		prevPeak = ph.getPeakWithDecay(now)

		// Always update current observed value
		ph.current = absDrift

		// Get the decayed peak value
		decayedPeak := prevPeak

		// If new value exceeds decayed peak, set new peak
		if absDrift > decayedPeak {
			ph.peak = absDrift
			ph.peakTime = now
		} else {
			// Update peak to the decayed value (so decay is persistent)
			ph.peak = decayedPeak
		}
	}

	currentPeak := ph.peak
	lslState.mu.Unlock()

	lslMs.WithLabelValues(peer).Set(currentMin)
	protocolOverheadMs.WithLabelValues(peer).Set(absDrift - currentMin)
	maxHlcDriftMs.WithLabelValues(peer).Set(currentPeak)

	// Causal horizon per §6.3 of the whitepaper: the point in time beyond which
	// no unobserved effect can exist is now + max_drift. An effect with HLC past
	// that horizon claims a timestamp that exceeds any previously observed drift,
	// indicating clock divergence or data corruption.
	causalHorizonMs.WithLabelValues(peer).Set(currentPeak)

	if exists {
		horizon := nowMs + prevPeak
		if driftSourceMs > horizon {
			causalityViolationsTotal.WithLabelValues(peer).Inc()
		}
	}
}

// --- 9.10 QUIC Transport ---

var (
	quicStreamsOpenedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_quic_streams_opened_total",
		Help: "Total QUIC uni-streams opened for notification/heartbeat sends",
	})

	quicStreamErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cluster_quic_stream_errors_total",
		Help: "Total QUIC uni-stream errors (open or read failures)",
	})
)

// RecordQUICStreamOpened increments the QUIC stream opened counter.
func RecordQUICStreamOpened() {
	quicStreamsOpenedTotal.Inc()
}

// RecordQUICStreamError increments the QUIC stream error counter.
func RecordQUICStreamError() {
	quicStreamErrorsTotal.Inc()
}

// RecordFetchServed increments the fetch served counter.
func RecordFetchServed() {
	FetchesServedTotal.Inc()
}

// RecordNotificationDropped increments the dropped notification counter for a peer.
func RecordNotificationDropped(peerID NodeId) {
	peerNotificationsDroppedTotal.WithLabelValues(peerLabel(peerID)).Inc()
}

// RecordPeerConnected sets the peer connection gauge to 1.
func RecordPeerConnected(peerID NodeId) {
	peerConnected.WithLabelValues(peerLabel(peerID)).Set(1)
}

// RecordPeerDisconnected sets the peer connection gauge to 0.
func RecordPeerDisconnected(peerID NodeId) {
	peerConnected.WithLabelValues(peerLabel(peerID)).Set(0)
}

// RecordPeerReconnect increments the reconnection counter for a peer.
func RecordPeerReconnect(peerID NodeId) {
	peerReconnectsTotal.WithLabelValues(peerLabel(peerID)).Inc()
}

func peerLabel(peerID NodeId) string {
	return fmt.Sprintf("%d", peerID)
}
