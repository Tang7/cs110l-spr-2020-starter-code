mod request;
mod response;

use clap::Parser;
use http::StatusCode;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time;

/// Contains information parsed from the command-line invocation of balancebeam. The Clap macros
/// provide a fancy way to automatically construct a command-line argument parser.
#[derive(Parser, Debug)]
#[command(about = "Fun with load balancing")]
struct CmdOptions {
    /// "IP/port to bind to"
    #[arg(short, long, default_value = "0.0.0.0:1100")]
    bind: String,
    /// "Upstream host to forward requests to"
    #[arg(short, long)]
    upstream: Vec<String>,
    /// "Perform active health checks on this interval (in seconds)"
    #[arg(long, default_value = "10")]
    active_health_check_interval: usize,
    /// "Path to send request to for active health checks"
    #[arg(long, default_value = "/")]
    active_health_check_path: String,
    /// "Maximum number of requests to accept per IP per minute (0 = unlimited)"
    #[arg(long, default_value = "0")]
    max_requests_per_minute: usize,
}

/// Contains information about the state of balancebeam (e.g. what servers we are currently proxying
/// to, what servers have failed, rate limiting counts, etc.)
///
/// You should add fields to this struct in later milestones.
#[derive(Clone)]
struct ProxyState {
    /// How frequently we check whether upstream servers are alive (Milestone 4)
    #[allow(dead_code)]
    active_health_check_interval: usize,
    /// Where we should send requests when doing active health checks (Milestone 4)
    #[allow(dead_code)]
    active_health_check_path: String,
    /// Maximum number of requests an individual IP can make in a minute (Milestone 5)
    #[allow(dead_code)]
    max_requests_per_minute: usize,
    /// Addresses of healthy servers that we are proxying to
    active_upstream_addresses: Arc<RwLock<Vec<String>>>,
    /// Addresses of all servers
    upstream_addresses: Vec<String>,
    /// Rate monitor, counts access number for each upstream address per minute
    rate_monitor: Arc<Mutex<HashMap<String, usize>>>,
}

#[tokio::main]
async fn main() {
    // Initialize the logging library. You can print log messages using the `log` macros:
    // https://docs.rs/log/0.4.8/log/ You are welcome to continue using print! statements; this
    // just looks a little prettier.
    if let Err(_) = std::env::var("RUST_LOG") {
        std::env::set_var("RUST_LOG", "debug");
    }
    pretty_env_logger::init();

    // Parse the command line arguments passed to this program
    let options = CmdOptions::parse();
    if options.upstream.len() < 1 {
        log::error!("At least one upstream server must be specified using the --upstream option.");
        std::process::exit(1);
    }

    // Start listening for connections
    let listener = match TcpListener::bind(&options.bind).await {
        Ok(listener) => listener,
        Err(err) => {
            log::error!("Could not bind to {}: {}", options.bind, err);
            std::process::exit(1);
        }
    };
    log::info!("Listening for requests on {}", options.bind);

    // Handle incoming connections
    let state = ProxyState {
        upstream_addresses: options.upstream.clone(),
        active_upstream_addresses: Arc::new(RwLock::new(options.upstream)),
        active_health_check_interval: options.active_health_check_interval,
        active_health_check_path: options.active_health_check_path,
        max_requests_per_minute: options.max_requests_per_minute,
        rate_monitor: Arc::new(Mutex::new(HashMap::new())),
    };

    start_health_check(&state);

    start_rate_monitor(&state);

    loop {
        if let Ok((stream, _)) = listener.accept().await {
            let state_ref = state.clone();
            tokio::spawn(async move {
                handle_connection(stream, &state_ref).await;
            });
        }
    }
}

async fn connect_to_upstream(state: &ProxyState) -> Result<TcpStream, std::io::Error> {
    // Keep connecting to active upstreams.
    loop {
        let mut rng = rand::rngs::StdRng::from_entropy();
        let upstream_idx = rng.gen_range(0..state.active_upstream_addresses.read().await.len());
        let upstream_ip = &state
            .active_upstream_addresses
            .read()
            .await
            .get(upstream_idx)
            .unwrap()
            .clone();

        match TcpStream::connect(upstream_ip).await {
            Ok(stream) => return Ok(stream),
            Err(err) => {
                log::error!("Failed to connect to upstream {}: {}", upstream_ip, err);
                state
                    .active_upstream_addresses
                    .write()
                    .await
                    .remove(upstream_idx);
                // Return error only when there is no active upstream left.
                if state.active_upstream_addresses.read().await.len() == 0 {
                    return Err(err);
                }
            }
        }
    }
}

async fn send_response(client_conn: &mut TcpStream, response: &http::Response<Vec<u8>>) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!(
        "{} <- {}",
        client_ip,
        response::format_response_line(&response)
    );
    if let Err(error) = response::write_to_stream(&response, client_conn).await {
        log::warn!("Failed to send response to client: {}", error);
        return;
    }
}

async fn handle_connection(mut client_conn: TcpStream, state: &ProxyState) {
    let client_ip = client_conn.peer_addr().unwrap().ip().to_string();
    log::info!("Connection received from {}", client_ip);

    // Open a connection to a random destination server
    let mut upstream_conn = match connect_to_upstream(state).await {
        Ok(stream) => stream,
        Err(_error) => {
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response).await;
            return;
        }
    };
    let upstream_ip = upstream_conn.peer_addr().unwrap().ip().to_string();

    // The client may now send us one or more requests. Keep trying to read requests until the
    // client hangs up or we get an error.
    loop {
        // Read a request from the client
        let mut request = match request::read_from_stream(&mut client_conn).await {
            Ok(request) => request,
            // Handle case where client closed connection and is no longer sending requests
            Err(request::Error::IncompleteRequest(0)) => {
                log::debug!("Client finished sending requests. Shutting down connection");
                return;
            }
            // Handle I/O error in reading from the client
            Err(request::Error::ConnectionError(io_err)) => {
                log::info!("Error reading request from client stream: {}", io_err);
                return;
            }
            Err(error) => {
                log::debug!("Error parsing request: {:?}", error);
                let response = response::make_http_error(match error {
                    request::Error::IncompleteRequest(_)
                    | request::Error::MalformedRequest(_)
                    | request::Error::InvalidContentLength
                    | request::Error::ContentLengthMismatch => http::StatusCode::BAD_REQUEST,
                    request::Error::RequestBodyTooLarge => http::StatusCode::PAYLOAD_TOO_LARGE,
                    request::Error::ConnectionError(_) => http::StatusCode::SERVICE_UNAVAILABLE,
                });
                send_response(&mut client_conn, &response).await;
                continue;
            }
        };
        log::info!(
            "{} -> {}: {}",
            client_ip,
            upstream_ip,
            request::format_request_line(&request)
        );

        // When reach rate limit, respond to request with HTTP error 429 (Too Many Requests)
        // rather than forwarding the requests to the upstream servers.
        if let Err(status) = check_rate_limit(state, &upstream_ip).await {
            let response = response::make_http_error(status);
            send_response(&mut client_conn, &response).await;
            continue;
        }

        // Add X-Forwarded-For header so that the upstream server knows the client's IP address.
        // (We're the ones connecting directly to the upstream server, so without this header, the
        // upstream server will only know our IP, not the client's.)
        request::extend_header_value(&mut request, "x-forwarded-for", &client_ip);

        // Forward the request to the server
        if let Err(error) = request::write_to_stream(&request, &mut upstream_conn).await {
            log::error!(
                "Failed to send request to upstream {}: {}",
                upstream_ip,
                error
            );
            let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
            send_response(&mut client_conn, &response).await;
            return;
        }
        log::debug!("Forwarded request to server");

        // Read the server's response
        let response = match response::read_from_stream(&mut upstream_conn, request.method()).await
        {
            Ok(response) => response,
            Err(error) => {
                log::error!("Error reading response from server: {:?}", error);
                let response = response::make_http_error(http::StatusCode::BAD_GATEWAY);
                send_response(&mut client_conn, &response).await;
                return;
            }
        };
        // Forward the response to the client
        send_response(&mut client_conn, &response).await;
        log::debug!("Forwarded response to client");
    }
}

fn start_health_check(state: &ProxyState) {
    let state_ref = state.clone();
    tokio::spawn(async move {
        health_check(&state_ref).await;
    });
}

async fn health_check(state: &ProxyState) {
    loop {
        time::sleep(time::Duration::from_secs(
            state.active_health_check_interval as u64,
        ))
        .await;

        let mut active_servers = state.active_upstream_addresses.write().await;
        active_servers.clear();

        for upstream in &state.upstream_addresses {
            let request = http::Request::builder()
                .method(http::Method::GET)
                .uri(&state.active_health_check_path)
                .header("Host", upstream)
                .body(Vec::<u8>::new())
                .unwrap();

            match TcpStream::connect(upstream).await {
                Ok(mut stream) => {
                    if let Err(err) = request::write_to_stream(&request, &mut stream).await {
                        log::error!("failed to write to stream {}, {}", upstream, err);
                    }

                    if let Ok(resp) =
                        response::read_from_stream(&mut stream, &request.method()).await
                    {
                        if http::StatusCode::OK == resp.status() {
                            active_servers.push(upstream.clone());
                        }
                    } else {
                        log::error!("failed to receive OK status from stream {}", upstream)
                    }
                }
                Err(err) => {
                    log::error!("failed to connect to stream {}, {}", upstream, err);
                }
            }
        }
    }
}

fn start_rate_monitor(state: &ProxyState) {
    let state_ref = state.clone();
    tokio::spawn(async move {
        reset_rate_monitor(&state_ref).await;
    });
}

async fn reset_rate_monitor(state: &ProxyState) {
    time::sleep(time::Duration::from_secs(60)).await;

    state.rate_monitor.lock().await.clear();
}

async fn check_rate_limit(state: &ProxyState, upstream: &str) -> Result<(), StatusCode> {
    if state.max_requests_per_minute == 0 {
        return Ok(());
    }

    let mut rate_monitor = state.rate_monitor.lock().await;
    let rate = rate_monitor.entry(upstream.to_string()).or_default();
    *rate += 1;
    if *rate > state.max_requests_per_minute {
        log::error!("reach maximum limit for stream {}", upstream.to_string());
        return Err(http::StatusCode::TOO_MANY_REQUESTS);
    }

    Ok(())
}
