use super::Activated;
use super::Capture;
use super::Error;
use super::Packet;
use super::State;
use futures;
use mio::event::Evented;
use mio::unix::EventedFd;
use mio::{Poll, PollOpt, Ready, Token};
use std::io;
#[cfg(not(windows))]
use std::os::unix::io::RawFd;
use tokio_core;

pub struct SelectableFd {
    fd: RawFd,
}

impl Evented for SelectableFd {
    fn register(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        EventedFd(&self.fd).register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &Poll,
        token: Token,
        interest: Ready,
        opts: PollOpt,
    ) -> io::Result<()> {
        EventedFd(&self.fd).reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        EventedFd(&self.fd).deregister(poll)
    }
}

pub trait PacketCodec {
    type Type;
    fn decode<'a>(&mut self, packet: Packet<'a>) -> Result<Self::Type, Error>;
}

pub struct PacketStream<T: State + ?Sized, C> {
    cap: Capture<T>,
    fd: tokio_core::reactor::PollEvented<SelectableFd>,
    codec: C,
}

impl<T: Activated + ?Sized, C: PacketCodec> PacketStream<T, C> {
    pub fn new(
        cap: Capture<T>,
        fd: RawFd,
        handle: &tokio_core::reactor::Handle,
        codec: C,
    ) -> Result<PacketStream<T, C>, Error> {
        Ok(PacketStream {
            cap,
            fd: tokio_core::reactor::PollEvented::new(SelectableFd { fd }, handle)?,
            codec,
        })
    }
}

impl<'a, T: Activated + ?Sized, C: PacketCodec> futures::Stream for PacketStream<T, C> {
    type Item = C::Type;
    type Error = Error;
    fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
        let p = match self.cap.next_noblock(&mut self.fd) {
            Ok(t) => t,
            Err(Error::IoError(ref e)) if *e == ::std::io::ErrorKind::WouldBlock => {
                return Ok(futures::Async::NotReady);
            }
            Err(e) => return Err(e.into()),
        };
        let frame = self.codec.decode(p)?;
        Ok(futures::Async::Ready(Some(frame)))
    }
}
