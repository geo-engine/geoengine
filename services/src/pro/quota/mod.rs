use std::sync::mpsc::Receiver;

pub fn quota_manager(receiver: Receiver<()>) {
    std::thread::spawn(move || loop {
        receiver.recv().expect("Channel should never be dropped");
        log::info!("Quota");
    });
}
