// This is an inclusion point of Geo Engine Pro

use std::sync::mpsc::{sync_channel, Receiver, SyncSender};

use geoengine_datatypes::util::test::TestDefault;

use crate::{error, util::Result};

pub mod adapters;
pub mod meta;

#[derive(Clone)]
pub struct QuotaTracking {
    tx: SyncSender<()>,
}

impl QuotaTracking {
    pub fn new_pair() -> (Self, Receiver<()>) {
        let (tx, rx) = sync_channel(100);
        (Self { tx }, rx)
    }

    pub fn work_unit_done(&self) -> Result<()> {
        self.tx.send(()).map_err(|_| error::Error::MpscSend)
    }
}

impl TestDefault for QuotaTracking {
    fn test_default() -> Self {
        Self::new_pair().0
    }
}
