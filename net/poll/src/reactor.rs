//! Poll-based reactor. This is a single-threaded reactor using a `poll` loop.
use crossbeam_channel as chan;

use nakamoto_common::block::time::{LocalDuration, LocalTime};

use nakamoto_p2p::error::Error;
use nakamoto_p2p::protocol;
use nakamoto_p2p::protocol::{Command, DisconnectReason, Event, Io, Link};
use nakamoto_p2p::traits;
use nakamoto_p2p::traits::{Dialer, Protocol};

use log::*;

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::io;
use std::io::prelude::*;
use std::net;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::time::SystemTime;

use crate::socket::Socket;
use crate::time::TimeoutManager;

/// Maximum amount of time to wait for i/o.
const WAIT_TIMEOUT: LocalDuration = LocalDuration::from_mins(60);
/// Socket read buffer size.
const READ_BUFFER_SIZE: usize = 1024 * 192;

#[derive(Debug, PartialEq, Eq, Clone)]
enum Source {
    Peer(net::SocketAddr),
    Listener,
    Waker,
}

/// A single-threaded non-blocking reactor.
pub struct Reactor<R: Write + Read, E> {
    peers: HashMap<net::SocketAddr, Socket<R>>,
    connecting: HashSet<net::SocketAddr>,
    commands: chan::Receiver<Command>,
    publisher: E,
    sources: popol::Sources<Source>,
    waker: Waker,
    timeouts: TimeoutManager<()>,
    shutdown: chan::Receiver<()>,
}

/// The `R` parameter represents the underlying stream type, eg. `net::TcpStream`.
impl<R: Write + Read + AsRawFd, E> Reactor<R, E> {
    /// Register a peer with the reactor.
    fn register_peer(&mut self, addr: net::SocketAddr, stream: R, link: Link) {
        self.sources
            .register(Source::Peer(addr), &stream, popol::interest::ALL);
        self.peers.insert(addr, Socket::from(stream, addr, link));
    }

    /// Unregister a peer from the reactor.
    fn unregister_peer<P>(
        &mut self,
        addr: net::SocketAddr,
        reason: DisconnectReason,
        protocol: &mut P,
    ) where
        P: Protocol,
    {
        self.connecting.remove(&addr);
        self.sources.unregister(&Source::Peer(addr));
        self.peers.remove(&addr);

        protocol.disconnected(&addr, reason);
    }
}

#[derive(Clone)]
pub struct Waker(Arc<popol::Waker>);

impl Waker {
    fn new(sources: &mut popol::Sources<Source>) -> io::Result<Self> {
        let waker = Arc::new(popol::Waker::new(sources, Source::Waker)?);

        Ok(Self(waker))
    }
}

impl traits::Waker for Waker {
    fn wake(&self) -> io::Result<()> {
        self.0.wake()
    }
}

impl<E: protocol::event::Publisher> traits::Reactor<E> for Reactor<net::TcpStream, E> {
    type Waker = Waker;

    /// Construct a new reactor, given a channel to send events on.
    fn new(
        publisher: E,
        commands: chan::Receiver<Command>,
        shutdown: chan::Receiver<()>,
    ) -> Result<Self, io::Error> {
        let peers = HashMap::new();

        let mut sources = popol::Sources::new();
        let waker = Waker::new(&mut sources)?;
        let timeouts = TimeoutManager::new(LocalDuration::from_secs(1));
        let connecting = HashSet::new();

        Ok(Self {
            peers,
            connecting,
            sources,
            commands,
            publisher,
            waker,
            timeouts,
            shutdown,
        })
    }

    /// Run the given protocol with the reactor.
    fn run<P, D>(
        &mut self,
        listen_addrs: &[net::SocketAddr],
        mut protocol: P,
        mut dialer: D,
    ) -> Result<(), Error>
    where
        P: Protocol,
        D: Dialer,
    {
        let listener = if listen_addrs.is_empty() {
            None
        } else {
            let listener = self::listen(listen_addrs)?;
            let local_addr = listener.local_addr()?;

            self.sources
                .register(Source::Listener, &listener, popol::interest::READ);
            self.publisher.publish(Event::Listening(local_addr));

            info!("Listening on {}", local_addr);

            Some(listener)
        };

        info!("Initializing protocol..");

        let local_time = SystemTime::now().into();
        protocol.initialize(local_time);

        self.process(&mut protocol, &mut dialer, local_time);

        // I/O readiness events populated by `popol::Sources::wait_timeout`.
        let mut events = popol::Events::new();
        // Timeouts populated by `TimeoutManager::wake`.
        let mut timeouts = Vec::with_capacity(32);

        loop {
            let timeout = self
                .timeouts
                .next(SystemTime::now())
                .unwrap_or(WAIT_TIMEOUT)
                .into();

            trace!(
                "Polling {} source(s) and {} timeout(s), waking up in {:?}..",
                self.sources.len(),
                self.timeouts.len(),
                timeout
            );

            let result = self.sources.wait_timeout(&mut events, timeout); // Blocking.
            let local_time = SystemTime::now().into();

            protocol.tick(local_time);

            match result {
                Ok(()) => {
                    trace!("Woke up with {} source(s) ready", events.len());

                    for (source, ev) in events.iter() {
                        match source {
                            Source::Peer(addr) => {
                                if ev.errored || ev.hangup {
                                    // Let the subsequent read fail.
                                    trace!("{}: Socket error triggered: {:?}", addr, ev);
                                }
                                if ev.invalid {
                                    // File descriptor was closed and is invalid.
                                    // Nb. This shouldn't happen. It means the source wasn't
                                    // properly unregistered, or there is a duplicate source.
                                    error!("{}: Socket is invalid, removing", addr);

                                    self.sources.unregister(&Source::Peer(*addr));
                                    continue;
                                }

                                if ev.writable {
                                    self.handle_writable(addr, source, &mut protocol)?;
                                }
                                if ev.readable {
                                    self.handle_readable(addr, &mut protocol);
                                }
                            }
                            Source::Listener => loop {
                                if let Some(ref listener) = listener {
                                    let (conn, addr) = match listener.accept() {
                                        Ok((conn, addr)) => (conn, addr),
                                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                            break;
                                        }
                                        Err(e) => {
                                            error!("Accept error: {}", e.to_string());
                                            break;
                                        }
                                    };
                                    trace!("{}: Accepting peer connection", addr);

                                    conn.set_nonblocking(true)?;

                                    let local_addr = conn.local_addr()?;
                                    let link = Link::Inbound;

                                    self.register_peer(addr, conn, link);

                                    protocol.connected(addr, &local_addr, link);
                                }
                            },
                            Source::Waker => {
                                trace!("Woken up by waker ({} command(s))", self.commands.len());

                                // Exit reactor loop if a shutdown was received.
                                if let Ok(()) = self.shutdown.try_recv() {
                                    return Ok(());
                                }
                                popol::Waker::reset(ev.source).ok();

                                debug_assert!(!self.commands.is_empty());

                                for cmd in self.commands.try_iter() {
                                    protocol.command(cmd);
                                }
                            }
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::TimedOut => {
                    // Nb. The way this is currently used basically ignores which keys have
                    // timed out. So as long as *something* timed out, we wake the protocol.
                    self.timeouts.wake(local_time, &mut timeouts);

                    if !timeouts.is_empty() {
                        timeouts.clear();
                        protocol.wake();
                    }
                }
                Err(err) => return Err(err.into()),
            }
            self.process(&mut protocol, &mut dialer, local_time);
        }
    }

    /// Return a new waker.
    ///
    /// Used to wake up the main event loop.
    fn waker(&self) -> Waker {
        self.waker.clone()
    }
}

impl<E: protocol::event::Publisher> Reactor<net::TcpStream, E> {
    /// Process protocol state machine outputs.
    fn process<P, D>(&mut self, protocol: &mut P, dialer: &mut D, local_time: LocalTime)
    where
        P: Protocol,
        D: Dialer,
    {
        // Note that there may be messages destined for a peer that has since been
        // disconnected.
        for out in protocol.drain() {
            match out {
                Io::Write(addr) => {
                    if let Some(source) = self.sources.get_mut(&Source::Peer(addr)) {
                        source.set(popol::interest::WRITE);
                    }
                }
                Io::Connect(addr) => {
                    trace!("Connecting to {}...", &addr);

                    match dialer.dial(&addr) {
                        Ok(stream) => {
                            trace!("{:#?}", stream);

                            self.register_peer(addr, stream, Link::Outbound);
                            self.connecting.insert(addr);

                            protocol.attempted(&addr);
                        }
                        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                            // Ignore. We are already establishing a connection through
                            // this socket.
                        }
                        Err(err) => {
                            error!("{}: Connection error: {}", addr, err.to_string());

                            protocol.disconnected(
                                &addr,
                                DisconnectReason::ConnectionError(Arc::new(err)),
                            );
                        }
                    }
                }
                Io::Disconnect(addr, reason) => {
                    if let Some(peer) = self.peers.get(&addr) {
                        trace!("{}: Disconnecting: {}", addr, reason);

                        // Shutdown the connection, ignoring any potential errors.
                        // If the socket was already disconnected, this will yield
                        // an error that is safe to ignore (`ENOTCONN`). The other
                        // possible errors relate to an invalid file descriptor.
                        peer.disconnect().ok();

                        self.unregister_peer(addr, reason, protocol);
                    }
                }
                Io::Wakeup(timeout) => {
                    self.timeouts.register((), local_time + timeout);
                }
                Io::Event(event) => {
                    trace!("Event: {:?}", event);

                    self.publisher.publish(event);
                }
            }
        }
    }

    fn handle_readable<P>(&mut self, addr: &net::SocketAddr, protocol: &mut P)
    where
        P: Protocol,
    {
        // Nb. If the socket was readable and writable at the same time, and it was disconnected
        // during an attempt to write, it will no longer be registered and hence available
        // for reads.
        if let Some(socket) = self.peers.get_mut(addr) {
            let mut buffer = [0; READ_BUFFER_SIZE];

            trace!("{}: Socket is readable", addr);

            // Nb. Since `poll`, which this reactor is based on, is *level-triggered*,
            // we will be notified again if there is still data to be read on the socket.
            // Hence, there is no use in putting this socket read in a loop, as the second
            // invocation would likely block.
            match socket.read(&mut buffer) {
                Ok(count) => {
                    if count > 0 {
                        trace!("{}: Read {} bytes", addr, count);

                        protocol.received_bytes(addr, &buffer[..count]);
                    } else {
                        trace!("{}: Read 0 bytes", addr);
                        // If we get zero bytes read as a return value, it means the peer has
                        // performed an orderly shutdown.
                        socket.disconnect().ok();
                        self.unregister_peer(*addr, DisconnectReason::PeerDisconnected, protocol);
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    // This shouldn't normally happen, since this function is only called
                    // when there's data on the socket. We leave it here in case external
                    // conditions change.
                }
                Err(err) => {
                    trace!("{}: Read error: {}", addr, err.to_string());

                    socket.disconnect().ok();
                    self.unregister_peer(
                        *addr,
                        DisconnectReason::ConnectionError(Arc::new(err)),
                        protocol,
                    );
                }
            }
        }
    }

    fn handle_writable<P: Protocol>(
        &mut self,
        addr: &net::SocketAddr,
        source: &Source,
        protocol: &mut P,
    ) -> io::Result<()> {
        trace!("{}: Socket is writable", addr);

        let source = self.sources.get_mut(source).unwrap();
        let mut socket = self.peers.get_mut(addr).unwrap();

        // "A file descriptor for a socket that is connecting asynchronously shall indicate
        // that it is ready for writing, once a connection has been established."
        //
        // Since we perform a non-blocking connect, we're only really connected once the socket
        // is writable.
        if self.connecting.remove(addr) {
            let local_addr = socket.local_address()?;

            protocol.connected(socket.address, &local_addr, socket.link);
        }

        match protocol.write(addr, &mut socket) {
            // In this case, we've written all the data, we
            // are no longer interested in writing to this
            // socket.
            Ok(()) => {
                source.unset(popol::interest::WRITE);
            }
            // In this case, the write couldn't complete. Set
            // our interest to `WRITE` to be notified when the
            // socket is ready to write again.
            Err(err)
                if [io::ErrorKind::WouldBlock, io::ErrorKind::WriteZero].contains(&err.kind()) =>
            {
                source.set(popol::interest::WRITE);
            }
            Err(err) => {
                error!("{}: Write error: {}", addr, err.to_string());

                socket.disconnect().ok();
                self.unregister_peer(
                    *addr,
                    DisconnectReason::ConnectionError(Arc::new(err)),
                    protocol,
                );
            }
        }
        Ok(())
    }
}

// Listen for connections on the given address.
fn listen<A: net::ToSocketAddrs>(addr: A) -> Result<net::TcpListener, Error> {
    let sock = net::TcpListener::bind(addr)?;

    sock.set_nonblocking(true)?;

    Ok(sock)
}
