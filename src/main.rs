use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::fs::{File, OpenOptions};
use serde::{Serialize, Deserialize};


#[derive(Debug, Serialize, Deserialize)]
enum Command {
    SET {key: String, value: String},
    GET {key: String},
    DELETE {key: String}
}


fn replay_log() -> io::Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    
    // Try to open the log file (might not exist on first run)
    let file = match File::open("kvstore.log") {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(map);  // No log file = fresh start
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
            Command::GET { .. } => {
                // Ignore GET commands in log
            }
        }
    }
    
    Ok(map)
}

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

fn handle_client(stream: TcpStream, addr: SocketAddr, data: Arc<Mutex<HashMap<String, String>>>) -> io::Result<()> {
    println!("new client: {addr:?}");

    let mut stream_clone = stream.try_clone()?;
    let mut reader = BufReader::new(stream);

    loop {
        let mut buffer = String::new();
        let Ok(bytes_read) = reader.read_line(&mut buffer) else { break };

        if bytes_read == 0 { break; }

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

    println!("Client disconnected");
    Ok(())
}


fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379")
        .expect("Failed to bind to 127.0.0.1:6379 - is the port already in use?");
    
    println!("Server listening...");
    
    // 1. Replay log from disk
    let restored_map = replay_log().expect("Failed to replay log");
    println!("Recovered {} keys from log", restored_map.len());

    // 2. Compact log
    compact_log(&restored_map).expect("Failed to compact log");
    println!("Log compacted");

    // 3. Wrap in thread-safe container
    let database = Arc::new(Mutex::new(restored_map));

    // 4. Accept connections
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                let db = Arc::clone(&database);
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