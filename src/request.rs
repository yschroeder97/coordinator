use std::fmt::Debug;

pub struct Request<P: Debug, R> {
    pub payload: P,
    pub respond_to: flume::Sender<R>,
}

impl<P: Debug, R> Debug for Request<P, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Request{:?}", self.payload)
    }
}

impl<P: Debug, R> Request<P, R> {
    pub fn respond(&self, response: R) -> Result<(), flume::SendError<R>> {
        // Will not block, because this is used as oneshot channel
        self.respond_to.send(response)
    }
}
