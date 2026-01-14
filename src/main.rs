use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

fn handle_client(stream: TcpStream, addr: SocketAddr) -> io::Result<()> {
    println!("new client: {addr:?}");

    // Clone stream: one for reading, one for writing
    let mut stream_clone = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    loop {
        let mut buffer = String::new();
        let bytes_read = match reader.read_line(&mut buffer) {
            Ok(n) => n,
            Err(_) => break,
        };

        // EOF - client disconnected
        if bytes_read == 0 {
            break;
        }

        stream_clone.write_all(buffer.as_bytes())?;
        stream_clone.flush()?;
    }

    println!("Client disconnected");
    Ok(())
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .expect("Failed to bind to 127.0.0.1:6379 - is the port already in use?");
    
    println!("Server listening...");

    // Accept clients sequentially
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                if let Err(e) = handle_client(stream, addr) {
                    eprintln!("Error handling client: {e}");
                }
            }
            Err(e) => eprintln!("Error accepting connection: {e}"),
        }
    }
}