
pub enum Message<E, R> {
    Event(E),
    Request(R),
}
