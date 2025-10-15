use crate::coordinator::{CoordinatorEvent, CoordinatorRequest};
use crate::errors::BoxedError;
use crate::messages::Message;
use crate::requests::Request;
use flume::{Receiver, Sender};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct MessageBusSender<E, R> {
    sender: Sender<Message<E, R>>,
}

pub struct MessageBusReceiver<E, R> {
    receiver: Receiver<Message<E, R>>,
    batch_size: usize,
    timeout_duration: Duration,
    buf: Vec<Message<E, R>>,
    deadline: Instant,
}

impl<E: Send + Sync + 'static, R: Send + Sync + 'static> MessageBusSender<E, R> {
    pub async fn send(&self, event: E) -> Result<(), flume::SendError<Message<E, R>>> {
        self.sender.send_async(Message::Event(event)).await
    }

    pub fn send_blocking(&self, event: E) -> Result<(), flume::SendError<Message<E, R>>> {
        self.sender.send(Message::Event(event))
    }

    pub async fn ask<Payload, Response>(&self, payload: Payload) -> Result<Response, BoxedError>
    where
        Request<Payload, Response>: Into<R>,
    {
        let (tx, rx) = flume::bounded(1);
        let request = Request {
            payload,
            respond_to: tx,
        };
        self.sender
            .send_async(Message::Request(request.into()))
            .await?;
        Ok(rx.recv_async().await?)
    }

    pub fn ask_blocking<Payload, Response>(&self, payload: Payload) -> Result<Response, BoxedError>
    where
        Request<Payload, Response>: Into<R>,
    {
        let (tx, rx) = flume::bounded(1);
        let request = Request {
            payload,
            respond_to: tx,
        };
        self.sender.send(Message::Request(request.into()))?;
        Ok(rx.recv()?)
    }
}

impl<E: Send + Sync + 'static, R: Send + Sync + 'static> MessageBusReceiver<E, R> {
    pub fn recv(&mut self) -> Option<Vec<Message<E, R>>> {
        self.buf.clear();
        let now = Instant::now();
        // If processing the previous batch took longer than the last set deadline, update the deadline
        if self.deadline < now {
            self.deadline = now + self.timeout_duration;
        }

        while self.buf.len() < self.batch_size {
            match self.receiver.recv_deadline(self.deadline) {
                Ok(msg) => self.buf.push(msg),
                Err(err) => match err {
                    flume::RecvTimeoutError::Timeout => {
                        break;
                    }
                    flume::RecvTimeoutError::Disconnected => return None,
                },
            }
        }

        // Processing of the deadline starts -> reset
        self.deadline = Instant::now() + self.timeout_duration;
        Some(std::mem::take(&mut self.buf))
    }
}

pub fn message_bus<E: Send + Sync + 'static, R: Send + Sync + 'static>(
    batch_size: usize,
    timeout_duration: Duration,
) -> (MessageBusSender<E, R>, MessageBusReceiver<E, R>) {
    assert!(batch_size > 0, "Batch size must be greater than 0");
    let (tx, rx) = flume::bounded(batch_size);

    (
        MessageBusSender { sender: tx },
        MessageBusReceiver {
            receiver: rx,
            batch_size,
            timeout_duration,
            buf: Vec::with_capacity(batch_size),
            deadline: Instant::now(),
        },
    )
}

// Convenience function for creating a coordinator message bus
pub fn coordinator_message_bus(
    timeout_duration: Duration,
    batch_size: usize,
) -> (
    MessageBusSender<CoordinatorEvent, CoordinatorRequest>,
    MessageBusReceiver<CoordinatorEvent, CoordinatorRequest>,
) {
    message_bus(batch_size, timeout_duration)
}

#[cfg(test)]
mod message_bus_test {
    use crate::coordinator::{CoordinatorEvent, CoordinatorRequest};
    use crate::data_model::{DataType, LogicalSource, Schema};
    use crate::events::Heartbeat;
    use crate::message_bus::coordinator_message_bus;
    use crate::messages::Message;
    use crate::requests::CreateLogicalSource;
    use std::time::Duration;

    #[test]
    fn send_recv_single_event() {
        let (tx, mut rx) = coordinator_message_bus(Duration::from_secs(1), 1);

        let heartbeat = Heartbeat {
            worker_id: "worker-1".to_string(),
        };
        tx.send_blocking(heartbeat.into()).unwrap();

        let msgs = rx.recv().unwrap();
        assert_eq!(msgs.len(), 1);

        match msgs.first().unwrap() {
            Message::Event(CoordinatorEvent::Heartbeat(heartbeat)) => {
                assert_eq!(heartbeat.worker_id, "worker-1");
            }
            Message::Event(CoordinatorEvent::RegisterWorker(_)) => {
                panic!("Expected heartbeat, got register worker")
            }
            Message::Request(_) => panic!("Expected event, got request"),
        }
    }

    #[test]
    fn ask_respond() {
        let (tx, mut rx) = coordinator_message_bus(Duration::from_secs(1), 1);

        std::thread::spawn(move || {
            let msgs = rx.recv().unwrap();
            assert_eq!(msgs.len(), 1);

            if let Message::Request(CoordinatorRequest::CreateLogicalSource(req)) =
                msgs.into_iter().next().unwrap()
            {
                assert_eq!(req.payload.source_name, "source");
                let response = Ok(LogicalSource {
                    name: "source".to_string(),
                    schema: Schema::new(vec![("ts".to_string(), DataType::UINT64)]),
                });
                req.respond(response).unwrap();
            } else {
                panic!("Expected request, got event");
            }
        });

        let result = tx
            .ask_blocking(CreateLogicalSource {
                source_name: "source".to_string(),
                schema: Schema::new(vec![]),
            })
            .unwrap();

        assert!(result.is_ok());
        let source = result.unwrap();
        assert_eq!(source.name, "source");
    }

    #[test]
    fn batching_test() {
        let batch_size = 5;
        let (tx, mut rx) = coordinator_message_bus(Duration::from_millis(100), batch_size);

        for i in 0..batch_size {
            let heartbeat = Heartbeat {
                worker_id: format!("worker-{}", i),
            };
            tx.send_blocking(heartbeat.into()).unwrap();
        }

        let msgs = rx.recv().unwrap();
        assert_eq!(msgs.len(), batch_size);

        for (i, msg) in msgs.iter().enumerate() {
            match msg {
                Message::Event(CoordinatorEvent::Heartbeat(event)) => {
                    assert_eq!(event.worker_id, format!("worker-{}", i));
                }
                Message::Event(CoordinatorEvent::RegisterWorker(_)) => {
                    panic!("Expected heartbeat, got register worker")
                }
                Message::Request(_) => panic!("Expected event, got request"),
            }
        }

        for i in 0..3 {
            let heartbeat = Heartbeat {
                worker_id: format!("partial-{}", i),
            };
            tx.send_blocking(heartbeat.into()).unwrap();
        }

        let msgs = rx.recv().unwrap();
        assert_eq!(msgs.len(), 3);

        for (i, msg) in msgs.iter().enumerate() {
            match msg {
                Message::Event(CoordinatorEvent::Heartbeat(event)) => {
                    assert_eq!(event.worker_id, format!("partial-{}", i));
                }
                Message::Event(CoordinatorEvent::RegisterWorker(_)) => {
                    panic!("Expected heartbeat, got register worker")
                }
                Message::Request(_) => panic!("Expected event, got request"),
            }
        }
    }
}
