use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
enum Command {
    SET {key: String, value: String},
    GET {key: String},
    DELETE {key: String}
}

// Parse input string into a Command or return an error message
fn parse_command(input: &str) -> Result<Command, String> {
    let parts: Vec<&str> = input.split_whitespace().collect();
    
    if parts.is_empty() {
        return Err("ERROR: Empty command".to_string());
    }
    
    let cmd = parts[0].to_uppercase();
    
    // Match command and validate argument count
    match (cmd.as_str(), parts.len()) {
        ("SET", 3) => Ok(Command::SET {
            key: parts[1].to_string(),
            value: parts[2].to_string(),
        }),
        ("SET", _) => Err("ERROR: SET requires a key and value".to_string()),
        
        ("GET", 2) => Ok(Command::GET {
            key: parts[1].to_string(),
        }),
        ("GET", _) => Err("ERROR: GET requires a key".to_string()),
        
        ("DELETE", 2) => Ok(Command::DELETE {
            key: parts[1].to_string(),
        }),
        ("DELETE", _) => Err("ERROR: DELETE requires a key".to_string()),
        
        _ => Err("ERROR: Unknown command".to_string()),
    }
}

// Handle a single client connection
fn handle_client(stream: TcpStream, addr: SocketAddr, data: Arc<Mutex<HashMap<String, String>>>) -> io::Result<()> {
    println!("new client: {addr:?}");

    let mut stream_clone = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    loop {
        let mut buffer = String::new();
        let Ok(bytes_read) = reader.read_line(&mut buffer) else { break };

        if bytes_read == 0 { break; } // Client disconnected

        match parse_command(&buffer) {
            Ok(Command::SET { key, value }) => {
                let mut map = data.lock().unwrap();
                map.insert(key, value);
                drop(map); // Release lock before I/O
                stream_clone.write_all(b"OK\n")?;
                stream_clone.flush()?;
            }
    
            Ok(Command::GET { key }) => {
                let map = data.lock().unwrap();
                let response = match map.get(&key) {
                    Some(value) => format!("{}\n", value),
                    None => "(nil)\n".to_string(),
                };
                drop(map); // Release lock before I/O
                stream_clone.write_all(response.as_bytes())?;
                stream_clone.flush()?;
            }
    
            Ok(Command::DELETE { key }) => {
                let mut map = data.lock().unwrap();
                let response = match map.remove(&key) {
                    Some(_) => "OK\n",
                    None => "(nil)\n",
                };
                drop(map); // Release lock before I/O
                stream_clone.write_all(response.as_bytes())?;
                stream_clone.flush()?;
            }
    
            Err(error_msg) => {
                stream_clone.write_all(error_msg.as_bytes())?;
                stream_clone.write_all(b"\n")?;
                stream_clone.flush()?;
            }
        }
    }

    println!("Client disconnected");
    Ok(())
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .expect("Failed to bind to 127.0.0.1:6379 - is the port already in use?");
    
    println!("Server listening...");
    
    // Shared in-memory storage (thread-safe)
    let database = Arc::new(Mutex::new(HashMap::new()));

    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                let db = Arc::clone(&database);
                // Spawn a thread for each client
                std::thread::spawn(move || {
                    if let Err(e) = handle_client(stream, addr, db) {
                        eprintln!("Error handling client: {e}");
                    }
                });
            }
            Err(e) => eprintln!("Error accepting connection: {e}"),
        }
    }
}