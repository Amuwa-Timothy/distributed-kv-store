use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::fs::{File, OpenOptions};
use serde::{Serialize, Deserialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;


#[derive(Debug, Serialize, Deserialize)]
enum Command {
    SET {key: String, value: String},
    GET {key: String},
    DELETE {key: String}
}


// Replay WAL from disk to rebuild in-memory state
fn replay_log() -> io::Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    
    let file = match File::open("kvstore.log") {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(map);
        }
        Err(e) => return Err(e),
    };
    
    let reader = BufReader::new(file);
    
    for line in reader.lines() {
        let line = line?;

        let command: Command = match serde_json::from_str(&line) {
            Ok(cmd) => cmd,
            Err(e) => {
                eprintln!("Warning: Skipped corrupted log entry: {}", e);
                continue;
            }
        };

        match command {
            Command::SET { key, value } => {
                map.insert(key, value);
            }
            Command::DELETE { key } => {
                map.remove(&key);
            }
            Command::GET { .. } => {}
        }
    }
    
    Ok(map)
}

// Compact WAL by rewriting only current state
fn compact_log(map: &HashMap<String, String>) -> io::Result<()> {
    let mut temp = File::create("kvstore.log.tmp")?;
    
    for (key, value) in map {
        let cmd = Command::SET { 
            key: key.clone(), 
            value: value.clone() 
        };
        let json = serde_json::to_string(&cmd)?;
        temp.write_all(json.as_bytes())?;
        temp.write_all(b"\n")?;
    }
    
    temp.sync_all()?;
    std::fs::rename("kvstore.log.tmp", "kvstore.log")?;
    
    Ok(())
}


fn parse_command(input: &str) -> Result<Command, String> {
    let parts: Vec<&str> = input.split_whitespace().collect();
    
    if parts.is_empty() {
        return Err("ERROR: Empty command".to_string());
    }
    
    let cmd = parts[0].to_uppercase();
    
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

// Append command to WAL (write-ahead for durability)
fn write_to_log(command: &Command) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("kvstore.log")?;

    let json = serde_json::to_string(command)?;
    file.write_all(json.as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_all()?;

    Ok(())
}

// Handle client connection in dedicated thread
fn handle_client(
    stream: TcpStream, 
    addr: SocketAddr, 
    shutdown: Arc<AtomicBool>, 
    data: Arc<Mutex<HashMap<String, String>>>
) -> io::Result<()> {
    println!("new client: {addr:?}");

    let mut stream_clone = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    // Timeout allows checking shutdown flag periodically
    stream_clone.set_read_timeout(Some(Duration::from_secs(1)))?;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            println!("Worker thread shutting down gracefully");
            break;
        }
    
        let mut buffer = String::new();
    
        match reader.read_line(&mut buffer) {
            Ok(0) => break, // Client disconnected
            Ok(_bytes_read) => {
                match parse_command(&buffer) {
                    Ok(Command::SET { key, value }) => {
                        write_to_log(&Command::SET { 
                            key: key.clone(), 
                            value: value.clone() 
                        })?;

                        let mut map = data.lock().unwrap();
                        map.insert(key, value);
                        drop(map);
                        
                        stream_clone.write_all(b"OK\n")?;
                        stream_clone.flush()?;
                    }
            
                    Ok(Command::GET { key }) => {
                        let map = data.lock().unwrap();
                        let response = match map.get(&key) {
                            Some(value) => format!("{}\n", value),
                            None => "(nil)\n".to_string(),
                        };
                        drop(map);
                        stream_clone.write_all(response.as_bytes())?;
                        stream_clone.flush()?;
                    }
            
                    Ok(Command::DELETE { key }) => {
                        write_to_log(&Command::DELETE { 
                            key: key.clone(), 
                        })?;

                        let mut map = data.lock().unwrap();
                        let response = match map.remove(&key) {
                            Some(_) => "OK\n",
                            None => "(nil)\n",
                        };
                        drop(map);
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
            Err(e) if e.kind() == io::ErrorKind::WouldBlock 
                   || e.kind() == io::ErrorKind::TimedOut => {
                continue; // Timeout - loop to check shutdown
            }
            Err(_) => break,
        }
    }

    println!("Client disconnected");
    Ok(())
}


fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .expect("Failed to bind");
    
    // Non-blocking allows shutdown check every 100ms
    listener.set_nonblocking(true).expect("Cannot set non-blocking");
    
    println!("Server listening...");
    
    let restored_map = replay_log().expect("Failed to replay log");
    println!("Recovered {} keys from log", restored_map.len());
    compact_log(&restored_map).expect("Failed to compact log");
    println!("Log compacted");

    let database = Arc::new(Mutex::new(restored_map));
    let shutdown = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    // Ctrl+C handler sets shutdown flag
    let shutdown_clone = Arc::clone(&shutdown);
    ctrlc::set_handler(move || {
        println!("\nShutdown signal received...");
        shutdown_clone.store(true, Ordering::Relaxed);
    }).expect("Error setting Ctrl+C handler");

    // Accept loop - checks shutdown every 100ms
    loop {
        if shutdown.load(Ordering::Relaxed) {
            println!("Stopping accept loop...");
            break;
        }

        match listener.accept() {
            Ok((stream, addr)) => {
                let db = Arc::clone(&database);
                let shutdown_flag = Arc::clone(&shutdown);
                let handle = std::thread::spawn(move || {
                    if let Err(e) = handle_client(stream, addr, shutdown_flag, db) {
                        eprintln!("Error handling client: {e}");
                    }
                });
                handles.push(handle);
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            Err(e) => eprintln!("Error accepting connection: {e}"),
        }
    }

    // Wait for all worker threads to finish
    println!("Waiting for {} active clients to finish...", handles.len());
    for handle in handles {
        handle.join().unwrap();
    }

    // Final cleanup: compact log before exit
    let final_map = database.lock().unwrap();
    compact_log(&final_map).expect("Failed to compact log on shutdown");
    println!("Server shutdown complete");
}