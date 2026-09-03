//! Tracks packet-capture health signals emitted by the capture backend.
//!
//! The `pktmon` backend drops packets when it can't keep up with a traffic
//! burst (notably Genshin's login data dump).  A lost KCP segment can't be
//! recovered by the passive sniffer, so the large login packets never
//! reassemble and the export ends up empty or partial.  The `pktmon` crate only
//! reports this by logging `warn!` events, so we watch the tracing stream and
//! count them, letting the UI tell the user to switch to the `pcap` backend.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

/// Shared, cheaply-cloneable counters describing how healthy the capture is.
#[derive(Clone, Default)]
pub struct CaptureHealth {
    /// Number of "missed packets" warnings seen from the capture backend.
    drop_events: Arc<AtomicU64>,
    /// Number of capture-stream warnings (e.g. oversized/truncated packets).
    stream_warnings: Arc<AtomicU64>,
}

impl CaptureHealth {
    pub fn drop_events(&self) -> u64 {
        self.drop_events.load(Ordering::Relaxed)
    }

    pub fn stream_warnings(&self) -> u64 {
        self.stream_warnings.load(Ordering::Relaxed)
    }

    /// True when the current capture has lost enough packets that the sniffed
    /// data is likely incomplete.
    pub fn is_lossy(&self) -> bool {
        self.drop_events() > 50 || self.stream_warnings() > 5
    }

    /// Reset the counters.  Called when the user (re)starts a capture.
    pub fn reset(&self) {
        self.drop_events.store(0, Ordering::Relaxed);
        self.stream_warnings.store(0, Ordering::Relaxed);
    }

    /// A [`tracing`] layer that keeps these counters up to date.
    pub fn layer(&self) -> CaptureHealthLayer {
        CaptureHealthLayer(self.clone())
    }
}

pub struct CaptureHealthLayer(CaptureHealth);

impl<S: Subscriber> Layer<S> for CaptureHealthLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if !event.metadata().target().starts_with("pktmon") {
            return;
        }

        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        if visitor.drop_event {
            self.0.drop_events.fetch_add(1, Ordering::Relaxed);
        }
        if visitor.stream_warning {
            self.0.stream_warnings.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    drop_event: bool,
    stream_warning: bool,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() != "message" {
            return;
        }
        let message = format!("{value:?}");
        if message.contains("missed reading packets") || message.contains("missed writing packets")
        {
            self.drop_event = true;
        } else if message.contains("stream warning reason") {
            self.stream_warning = true;
        }
    }
}
