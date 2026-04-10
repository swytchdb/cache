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
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// nextTestPeerID generates unique peer IDs across test runs in the same process,
// so that -count=N doesn't pollute lslState between iterations.
var nextTestPeerID atomic.Uint64

func init() {
	nextTestPeerID.Store(1000)
}

func uniquePeerID() NodeId {
	return NodeId(nextTestPeerID.Add(1))
}

func getCounterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	c.Write(&m)
	return m.GetCounter().GetValue()
}

func getGaugeValue(g prometheus.Gauge) float64 {
	var m dto.Metric
	g.Write(&m)
	return m.GetGauge().GetValue()
}

func TestRecordNotificationSent(t *testing.T) {
	before := getCounterValue(NotificationsSentTotal)
	RecordNotificationSent()
	after := getCounterValue(NotificationsSentTotal)
	if after != before+1 {
		t.Fatalf("expected counter to increment by 1, got %f -> %f", before, after)
	}
}

func TestRecordNotificationReceived(t *testing.T) {
	before := getCounterValue(NotificationsReceivedTotal)
	now := time.Now()
	RecordNotificationReceived(42, now, uint64(now.UnixNano()))
	after := getCounterValue(NotificationsReceivedTotal)
	if after != before+1 {
		t.Fatalf("expected counter to increment by 1, got %f -> %f", before, after)
	}
}

func TestRecordFetchServed(t *testing.T) {
	before := getCounterValue(FetchesServedTotal)
	RecordFetchServed()
	after := getCounterValue(FetchesServedTotal)
	if after != before+1 {
		t.Fatalf("expected counter to increment by 1, got %f -> %f", before, after)
	}
}

func TestRecordPeerConnectedDisconnected(t *testing.T) {
	RecordPeerConnected(1)
	g := peerConnected.WithLabelValues("1")
	val := getGaugeValue(g)
	if val != 1 {
		t.Fatalf("expected peer_connected=1, got %f", val)
	}

	RecordPeerDisconnected(1)
	val = getGaugeValue(g)
	if val != 0 {
		t.Fatalf("expected peer_connected=0, got %f", val)
	}
}

func TestRecordNotificationDropped(t *testing.T) {
	RecordNotificationDropped(5)
	c := peerNotificationsDroppedTotal.WithLabelValues("5")
	var m dto.Metric
	c.Write(&m)
	if m.GetCounter().GetValue() < 1 {
		t.Fatal("expected notifications_dropped counter >= 1")
	}
}

func TestRecordPeerReconnect(t *testing.T) {
	RecordPeerReconnect(3)
	c := peerReconnectsTotal.WithLabelValues("3")
	var m dto.Metric
	c.Write(&m)
	if m.GetCounter().GetValue() < 1 {
		t.Fatal("expected reconnects counter >= 1")
	}
}

func TestCausalityViolationNotTriggeredForNormalDrift(t *testing.T) {
	peerID := uniquePeerID()

	before := getCounterValue(causalityViolationsTotal.WithLabelValues(peerLabel(peerID)))

	// First message: send_time 100ms in the past (normal network delay).
	// This establishes max_drift = 100ms.
	now := time.Now()
	RecordNotificationReceived(peerID, now, uint64(now.Add(-100*time.Millisecond).UnixNano()))

	// Second message: send_time 50ms in the past (less drift than max).
	now = time.Now()
	RecordNotificationReceived(peerID, now, uint64(now.Add(-50*time.Millisecond).UnixNano()))

	after := getCounterValue(causalityViolationsTotal.WithLabelValues(peerLabel(peerID)))
	if after != before {
		t.Fatalf("expected no causality violations for normal drift, got %f new violations", after-before)
	}
}

func TestCausalityViolationNotTriggeredForModestFutureDrift(t *testing.T) {
	peerID := uniquePeerID()

	before := getCounterValue(causalityViolationsTotal.WithLabelValues(peerLabel(peerID)))

	// First message: send_time 200ms in the past. Establishes max_drift = 200ms.
	now := time.Now()
	RecordNotificationReceived(peerID, now, uint64(now.Add(-200*time.Millisecond).UnixNano()))

	// Second message: send_time 100ms in the FUTURE. This is within the horizon
	// (now + 200ms), so it should NOT be a violation.
	now = time.Now()
	RecordNotificationReceived(peerID, now, uint64(now.Add(100*time.Millisecond).UnixNano()))

	after := getCounterValue(causalityViolationsTotal.WithLabelValues(peerLabel(peerID)))
	if after != before {
		t.Fatalf("expected no causality violation for drift within horizon, got %f new violations", after-before)
	}
}

func TestCausalityViolationTriggeredBeyondHorizon(t *testing.T) {
	peerID := uniquePeerID()

	before := getCounterValue(causalityViolationsTotal.WithLabelValues(peerLabel(peerID)))

	// First message: send_time 50ms in the past. Establishes max_drift = 50ms.
	now := time.Now()
	RecordNotificationReceived(peerID, now, uint64(now.Add(-50*time.Millisecond).UnixNano()))

	// Second message: send_time 500ms in the FUTURE. This far exceeds the horizon
	// (now + max_drift where max_drift was 50ms), so it IS a violation.
	now = time.Now()
	RecordNotificationReceived(peerID, now, uint64(now.Add(500*time.Millisecond).UnixNano()))

	after := getCounterValue(causalityViolationsTotal.WithLabelValues(peerLabel(peerID)))
	if after <= before {
		t.Fatal("expected causality violation for send_time far beyond horizon")
	}
}

func TestCausalHorizonGaugeReflectsMaxDrift(t *testing.T) {
	peerID := uniquePeerID()
	peer := peerLabel(peerID)

	// Send a message with send_time 150ms in the past. max_drift = 150ms.
	now := time.Now()
	RecordNotificationReceived(peerID, now, uint64(now.Add(-150*time.Millisecond).UnixNano()))

	horizonVal := getGaugeValue(causalHorizonMs.WithLabelValues(peer))
	// The gauge should reflect max_drift (~150ms), not an absolute timestamp.
	if horizonVal < 100 || horizonVal > 250 {
		t.Fatalf("expected causal horizon gauge ~150 (max_drift), got %f", horizonVal)
	}
}
