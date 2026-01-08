use std::fmt::Debug;
use tokio::sync::oneshot;

pub struct Request<P: Debug, R> {
    pub payload: P,
    pub reply_to: oneshot::Sender<R>,
}

impl<P: Debug, R> Debug for Request<P, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Request{:?}", self.payload)
    }
}

impl<P: Debug, Rsp> Request<P, Rsp> {
    pub fn new(payload: P) -> (oneshot::Receiver<Rsp>, Self) {
        let (tx, rx) = oneshot::channel();
        (
            rx,
            Self {
                payload,
                reply_to: tx,
            },
        )
    }

    pub fn reply(self, response: Rsp) -> Result<(), Rsp> {
        // Will not block, because this is used as oneshot channel
        self.reply_to.send(response)
    }
}
