use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::net::SocketAddrV4;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::{io, mem};

use io_uring::types::Fd;
use io_uring::{opcode, CompletionQueue, SubmissionQueue, Submitter};
use libc::{c_int, sockaddr, socklen_t};
use socket2::SockAddr;

const RESPONSE: &'static [u8] =
    b"HTTP/1.1 200 OK\nContent-Type: text/plain\nContent-Length: 12\n\nHello world!";

const BACKLOG: c_int = 256;

const URING_QUEUE_SIZE: u32 = 256;

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
#[repr(u32)]
enum EventKind {
    Accept = 0,
    Write = 1,
    Close = 2,
}

#[repr(packed)]
#[derive(Clone, Copy)]
struct UserData {
    kind: EventKind,
    id: u32,
}

struct AddrPinned {
    addr: UnsafeCell<MaybeUninit<sockaddr>>,
    len: UnsafeCell<MaybeUninit<socklen_t>>,
}

fn main() {
    let listener = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .unwrap();

    listener
        .bind(&SockAddr::from(SocketAddrV4::new(
            "127.0.0.1".parse().unwrap(),
            8080,
        )))
        .unwrap();

    listener.listen(BACKLOG).unwrap();

    let mut uring = io_uring::IoUring::new(URING_QUEUE_SIZE).unwrap();

    uring.submission();

    let mut counter = 0;
    let mut addr_store = HashMap::new();

    let user_data = UserData {
        kind: EventKind::Accept,
        id: counter,
    };

    let (mut submitter, mut squeue, mut completions) = uring.split();

    for _ in 0..64 {
        unsafe {
            submit_accept(
                &listener,
                &mut submitter,
                &mut squeue,
                user_data,
                &mut addr_store,
            )
            .unwrap()
        }

        counter += 1;
    }

    loop {
        unsafe {
            handle_completions(
                &listener,
                &mut counter,
                &mut submitter,
                &mut squeue,
                &mut completions,
                &mut addr_store,
            );
        }
    }
}

unsafe fn handle_completions(
    listener: &socket2::Socket,
    counter: &mut u32,
    submitter: &mut Submitter,
    squeue: &mut SubmissionQueue,
    completions: &mut CompletionQueue,
    store: &mut HashMap<u32, Box<AddrPinned>>,
) {
    submitter.submit_and_wait(1).unwrap();
    completions.sync();

    for c in completions {
        let user_data: UserData = mem::transmute(c.user_data());

        match user_data.kind {
            EventKind::Accept => {
                if c.result() > 0 {
                    let data = user_data.id;
                    let _ = store.remove(&data);
                    let sock = socket2::Socket::from_raw_fd(c.result());
                    submit_send(&sock, submitter, squeue).unwrap();
                    mem::forget(sock);
                }

                let user_data = UserData {
                    kind: EventKind::Accept,
                    id: *counter,
                };

                *counter += 1;

                submit_accept(&listener, submitter, squeue, user_data, store).unwrap()
            }
            EventKind::Write => {
                let sock = socket2::Socket::from_raw_fd(user_data.id as i32);
                submit_close(&sock, submitter, squeue).unwrap();
                mem::forget(sock);
            }
            EventKind::Close => {}
        }
    }
}

unsafe fn submit_close(
    stream: &socket2::Socket,
    submitter: &mut Submitter,
    squeue: &mut SubmissionQueue,
) -> io::Result<()> {
    let user_data = UserData {
        kind: EventKind::Close,
        id: 0,
    };

    let entry = opcode::Close::new(Fd(stream.as_raw_fd()))
        .build()
        .user_data(mem::transmute(user_data));

    while squeue.push(&entry).is_err() {
        submitter.submit().unwrap();
    }

    squeue.sync();
    Ok(())
}

unsafe fn submit_accept(
    listener: &socket2::Socket,
    submitter: &mut Submitter,
    squeue: &mut SubmissionQueue,
    data: UserData,
    store: &mut HashMap<u32, Box<AddrPinned>>,
) -> io::Result<()> {
    let addr = Box::new(AddrPinned {
        addr: UnsafeCell::new(MaybeUninit::uninit()),
        len: UnsafeCell::new(MaybeUninit::uninit()),
    });

    let entry = opcode::Accept::new(
        Fd(listener.as_raw_fd()),
        addr.addr.get() as _,
        addr.len.get() as _,
    )
    .build()
    .user_data(mem::transmute(data));

    while squeue.push(&entry).is_err() {
        submitter.submit().unwrap();
    }

    squeue.sync();
    store.insert(data.id, addr);
    Ok(())
}

unsafe fn submit_send(
    stream: &socket2::Socket,
    submitter: &mut Submitter,
    squeue: &mut SubmissionQueue,
) -> io::Result<()> {
    let user_data = UserData {
        kind: EventKind::Write,
        id: stream.as_raw_fd() as u32,
    };

    let entry = opcode::Send::new(
        Fd(stream.as_raw_fd()),
        RESPONSE.as_ptr(),
        RESPONSE.len() as u32,
    )
    .build()
    .user_data(mem::transmute(user_data));

    while squeue.push(&entry).is_err() {
        println!("Failed to push send");
        submitter.submit().unwrap();
    }

    squeue.sync();
    Ok(())
}
