pub struct Request<P, R> {
    pub payload: P,
    pub respond_to: flume::Sender<R>,
}

impl<P, R> Request<P, R> {
    pub fn respond(&self, response: R) -> Result<(), flume::SendError<R>> {
        // Will not block, because this is used as oneshot channel
        self.respond_to.send(response)
    }
}
