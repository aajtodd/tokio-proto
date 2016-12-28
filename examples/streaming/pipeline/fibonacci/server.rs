//
// This example demonstrates a (simple) example of a `streaming::pipeline` server
// using Framed and Codec.
//
extern crate futures;
extern crate tokio_core;
extern crate tokio_proto;
extern crate tokio_service;

use std::str;
use std::io::{self, ErrorKind, Write};
use std::sync::{Arc, Mutex};
use std::{thread, time};

use futures::{future, Future, BoxFuture, Sink};
use tokio_core::io::{Io, Codec, Framed, EasyBuf};
use tokio_proto::TcpServer;
use tokio_proto::streaming::pipeline::{ServerProto, Frame};
use tokio_proto::streaming::{Message, Body};
use tokio_service::Service;

#[derive(Debug)]
pub enum Request {
    GetNext(u64),           
}

impl From<u64> for Request {
    fn from(n: u64) -> Self {
        Request::GetNext(n)
    }
}


#[derive(Default)]
pub struct IntCodec;

fn parse_u64(from: &[u8]) -> Result<u64, io::Error> {
    println!("parse buf: {:?}", from);
    Ok(str::from_utf8(from)
       .map_err(|e| {
           println!("error: {}", e);
           io::Error::new(ErrorKind::InvalidData, e)
       })?
       .parse()
       .map_err(|e| {
           println!("error: {}", e);
           io::Error::new(ErrorKind::InvalidData, e)
       })?)
}

impl Codec for IntCodec {
    // Note the types for `In` and `Out` here are Frames that match the corresponding
    // ServerProto definitions: 
    // `Frame<Request, RequestBody, Error>` and 
    // `Frame<Response, ResponseBody, Error>` respectively.
    type In = Frame<Request, (), io::Error>;
    type Out = Frame<u64, u64, io::Error>;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        if let Some(i) = buf.as_slice().iter().position(|&b| b == b'\n') {
            // remove the line including the '\n', from the buffer
            let full_line = buf.drain_to(i + 1);

            // strip the '\n'
            let slice = &full_line.as_slice()[..i];

            // attempt to parse and match the request type
            let req = Request::from(parse_u64(slice)?);

            let frame = Frame::Message { message: req, body: false };
            Ok(Some(frame))
        } else {
            Ok(None)
        }
    }

    fn decode_eof(&mut self, buf: &mut EasyBuf) -> io::Result<Self::In> {
        let amt = buf.len();
        let req = Request::from(parse_u64(buf.drain_to(amt).as_slice())?);
        let frame = Frame::Message { message: req, body: false };
        Ok(frame)
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        // Encode will be called each time there is a new message to write to the
        // wire. We match against the incoming message type and serialize it
        // as appropriate. 
        // See the definitions of `pipeline::Frame` for more information.
        match msg {
            Frame::Message {message, .. } => {
                println!("encode message!");
                writeln!(buf, "{}", message);
            },
            Frame::Body {chunk} => {
                if let Some(m) = chunk {
                    println!("encode body!");
                    writeln!(buf, "{}", m);
                }
            },
            Frame::Error{error} => {
                println!("encode, error! : {}", error);
                return Err(error);
            }
        }
        Ok(())
    }
}

struct IntProto;

// Next, we implement the server protocol
impl <T: Io + 'static> ServerProto<T> for IntProto {
    type Request = Request;
    type RequestBody = ();
    type Response = u64;
    type ResponseBody = u64;
    type Error = io::Error;

    // We make use of `Framed` from tokio-core here. This works for simple protocols. If
    // you find yourself needing more flexibility you will have to implement `Stream`, `Sink`, 
    // and `Transport` yourself.
    type Transport = Framed<T, IntCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(IntCodec))
    }
}

// Now we implement a service we'd like to run on top of this protocol.
// This toy service generates fibonacci numbers, it takes a single integer 'n'
// and will generate the next 'n' fibonacci numbers as a stream
pub struct Fibonacci {
    state: Arc<Mutex<State>>,
}

impl Fibonacci { 
    fn new() -> Self {
        Fibonacci {
            state: Arc::new(Mutex::new(State::new())),
        }
    }

    fn start_stream(&self, n: u64) -> Body<u64, io::Error> {
        let (mut tx, rx) = Body::<u64, io::Error>::pair();
        let state = self.state.clone();

        thread::spawn(move || {
            for _ in 0..n {
                // pull the next number out
                let res = state.lock().unwrap().next().unwrap();

                // Send the next value into the stream.
                // We are not on the event loop thread, we have to call `wait`
                // to pump the message through.
                let send_result = tx.send(Ok(res)).wait();
                if send_result.is_err() {
                    println!("error sending");
                    return;
                }
                // the sender is returned in the result
                tx = send_result.unwrap();
                thread::sleep(time::Duration::from_secs(1));
            }
            println!("stream done");
        });

        // return the body part of the stream
        rx
    }

    fn next_number(&self) -> u64 {
        self.state.lock().unwrap().next().unwrap()   
    }
}

pub struct State {
    curr: u64,
    next: u64
}

impl State {
    fn new() -> Self {
        State {
            curr: 0,
            next: 1
        }
    }
}

impl Iterator for State {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        let new_next = self.curr + self.next;
        self.curr = self.next;
        self.next = new_next;
        Some(self.curr)
    }
}

impl Service for Fibonacci {
    type Request = Message<Request, Body<(), io::Error>>;
    type Response = Message<u64, Body<u64, io::Error>>;
    type Error = io::Error;
    type Future = BoxFuture<Self::Response, io::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        println!("Service::call - req: {:?}", *req);

        match *req {
            Request::GetNext(0) | Request::GetNext(1) => {
                // return the next number immediately without a body
                return future::finished(Message::WithoutBody(self.next_number())).boxed();

            }
            Request::GetNext(n) => {
                let next = self.next_number();
                let body = self.start_stream(n-1);

                // here we return the next value as well as the `Body` which is a stream of future
                // values yet to be generated. Each time this stream has a new value it will be
                // sent to the client
                return future::finished(Message::WithBody(next, body)).boxed();
            },
        }
    }
}

// Finally, we can actually host this service locally!
fn main() {
    let addr = "0.0.0.0:12345".parse().unwrap();
    println!("listening on: {}", addr);
    TcpServer::new(IntProto, addr)
        .serve(|| Ok(Fibonacci::new()));
}
