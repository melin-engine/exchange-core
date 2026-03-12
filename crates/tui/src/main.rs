use std::net::SocketAddr;
use std::num::NonZeroU64;

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph};
use tokio::sync::mpsc;

use trading_client::Client;
use trading_protocol::message::{Request, Response};
use trading_protocol::types::{
    AccountId, ExecutionReport, Order, OrderId, OrderType, Price, Quantity, RejectReason, Side,
    Symbol, TimeInForce,
};

/// TUI application state.
struct App {
    /// Log of formatted events (responses, errors, status messages).
    log: Vec<String>,
    /// Current input buffer for the command line.
    input: String,
    /// Channel to send requests to the client task.
    request_tx: mpsc::Sender<Request>,
    /// Channel to receive formatted response strings from the client task.
    response_rx: mpsc::Receiver<String>,
    /// Whether the app should quit.
    quit: bool,
    /// Auto-incrementing order ID counter.
    next_order_id: u64,
}

impl App {
    fn new(request_tx: mpsc::Sender<Request>, response_rx: mpsc::Receiver<String>) -> Self {
        Self {
            log: vec!["Type 'help' for available commands.".into()],
            input: String::new(),
            request_tx,
            response_rx,
            quit: false,
            next_order_id: 1,
        }
    }

    /// Drain pending responses from the client task into the log.
    fn poll_responses(&mut self) {
        while let Ok(msg) = self.response_rx.try_recv() {
            self.log.push(msg);
        }
    }

    /// Process the current input as a command.
    fn submit_command(&mut self) {
        let input = self.input.trim().to_string();
        self.input.clear();

        if input.is_empty() {
            return;
        }

        self.log.push(format!("> {input}"));

        let parts: Vec<&str> = input.split_whitespace().collect();
        match parts.first().copied() {
            Some("help") => {
                self.log.push("Commands:".into());
                self.log
                    .push("  buy <symbol> <price> <qty>   — limit buy order".into());
                self.log
                    .push("  sell <symbol> <price> <qty>  — limit sell order".into());
                self.log
                    .push("  mbuy <symbol> <qty>          — market buy order".into());
                self.log
                    .push("  msell <symbol> <qty>         — market sell order".into());
                self.log
                    .push("  cancel <symbol> <order_id>   — cancel order".into());
                self.log
                    .push("  quit                         — exit".into());
            }
            Some("quit") | Some("q") => {
                self.quit = true;
            }
            Some("buy") | Some("sell") => {
                self.handle_limit_order(&parts);
            }
            Some("mbuy") | Some("msell") => {
                self.handle_market_order(&parts);
            }
            Some("cancel") => {
                self.handle_cancel(&parts);
            }
            _ => {
                self.log.push("Unknown command. Type 'help'.".into());
            }
        }
    }

    fn handle_limit_order(&mut self, parts: &[&str]) {
        if parts.len() != 4 {
            self.log
                .push("Usage: buy|sell <symbol> <price> <qty>".into());
            return;
        }

        let side = if parts[0] == "buy" {
            Side::Buy
        } else {
            Side::Sell
        };
        let symbol = match parts[1].parse::<u32>() {
            Ok(s) => Symbol(s),
            Err(_) => {
                self.log.push("Invalid symbol (expected u32).".into());
                return;
            }
        };
        let price = match parts[2].parse::<u64>().ok().and_then(NonZeroU64::new) {
            Some(p) => Price(p),
            None => {
                self.log
                    .push("Invalid price (expected non-zero u64).".into());
                return;
            }
        };
        let quantity = match parts[3].parse::<u64>().ok().and_then(NonZeroU64::new) {
            Some(q) => Quantity(q),
            None => {
                self.log
                    .push("Invalid quantity (expected non-zero u64).".into());
                return;
            }
        };

        let order_id = OrderId(self.next_order_id);
        self.next_order_id += 1;

        let order = Order {
            id: order_id,
            account: AccountId(1),
            side,
            order_type: OrderType::Limit { price },
            time_in_force: TimeInForce::GTC,
            quantity,
        };

        let request = Request::SubmitOrder { symbol, order };
        self.log.push(format!(
            "Submitting limit {side:?} order #{order_id:?} @ {price:?} x {quantity:?}"
        ));
        if self.request_tx.try_send(request).is_err() {
            self.log
                .push("Failed to send request (disconnected).".into());
        }
    }

    fn handle_market_order(&mut self, parts: &[&str]) {
        if parts.len() != 3 {
            self.log.push("Usage: mbuy|msell <symbol> <qty>".into());
            return;
        }

        let side = if parts[0] == "mbuy" {
            Side::Buy
        } else {
            Side::Sell
        };
        let symbol = match parts[1].parse::<u32>() {
            Ok(s) => Symbol(s),
            Err(_) => {
                self.log.push("Invalid symbol (expected u32).".into());
                return;
            }
        };
        let quantity = match parts[2].parse::<u64>().ok().and_then(NonZeroU64::new) {
            Some(q) => Quantity(q),
            None => {
                self.log
                    .push("Invalid quantity (expected non-zero u64).".into());
                return;
            }
        };

        let order_id = OrderId(self.next_order_id);
        self.next_order_id += 1;

        let order = Order {
            id: order_id,
            account: AccountId(1),
            side,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::GTC,
            quantity,
        };

        let request = Request::SubmitOrder { symbol, order };
        self.log.push(format!(
            "Submitting market {side:?} order #{order_id:?} x {quantity:?}"
        ));
        if self.request_tx.try_send(request).is_err() {
            self.log
                .push("Failed to send request (disconnected).".into());
        }
    }

    fn handle_cancel(&mut self, parts: &[&str]) {
        if parts.len() != 3 {
            self.log.push("Usage: cancel <symbol> <order_id>".into());
            return;
        }

        let symbol = match parts[1].parse::<u32>() {
            Ok(s) => Symbol(s),
            Err(_) => {
                self.log.push("Invalid symbol (expected u32).".into());
                return;
            }
        };
        let order_id = match parts[2].parse::<u64>() {
            Ok(id) => OrderId(id),
            Err(_) => {
                self.log.push("Invalid order_id (expected u64).".into());
                return;
            }
        };

        let request = Request::CancelOrder { symbol, order_id };
        self.log.push(format!("Cancelling order #{}", order_id.0));
        if self.request_tx.try_send(request).is_err() {
            self.log
                .push("Failed to send request (disconnected).".into());
        }
    }
}

/// Format an execution report for display.
fn format_report(report: &ExecutionReport) -> String {
    match report {
        ExecutionReport::Placed {
            order_id,
            side,
            price,
            quantity,
        } => format!(
            "PLACED: order #{} {side:?} @ {price:?} x {quantity:?}",
            order_id.0
        ),
        ExecutionReport::Fill {
            maker_order_id,
            taker_order_id,
            price,
            quantity,
            ..
        } => format!(
            "FILL: maker #{} / taker #{} @ {price:?} x {quantity:?}",
            maker_order_id.0, taker_order_id.0
        ),
        ExecutionReport::Cancelled {
            order_id,
            remaining_quantity,
        } => format!(
            "CANCELLED: order #{} (remaining: {remaining_quantity:?})",
            order_id.0
        ),
        ExecutionReport::Triggered {
            order_id,
            trigger_price,
        } => format!("TRIGGERED: order #{} @ {trigger_price:?}", order_id.0),
        ExecutionReport::Rejected { order_id, reason } => {
            let reason_str = match reason {
                RejectReason::NoLiquidity => "no liquidity",
                RejectReason::FOKCannotFill => "FOK cannot fill",
                RejectReason::InsufficientBalance => "insufficient balance",
                RejectReason::UnknownAccount => "unknown account",
                RejectReason::UnknownSymbol => "unknown symbol",
            };
            format!("REJECTED: order #{} ({reason_str})", order_id.0)
        }
    }
}

/// Background task that owns the client connection, sends requests,
/// and forwards formatted responses back to the TUI.
async fn client_task(
    addr: SocketAddr,
    mut request_rx: mpsc::Receiver<Request>,
    response_tx: mpsc::Sender<String>,
) {
    let mut client = match Client::connect(addr).await {
        Ok(c) => {
            let _ = response_tx.send(format!("Connected to {addr}")).await;
            c
        }
        Err(e) => {
            let _ = response_tx.send(format!("Connection failed: {e}")).await;
            return;
        }
    };

    while let Some(request) = request_rx.recv().await {
        match client.send_request(&request).await {
            Ok(responses) => {
                for resp in &responses {
                    let msg = match resp {
                        Response::Report(report) => format_report(report),
                        Response::EngineError => "ENGINE ERROR".into(),
                        Response::BatchEnd => continue,
                    };
                    let _ = response_tx.send(msg).await;
                }
                if responses.is_empty() {
                    let _ = response_tx.send("(no reports)".into()).await;
                }
            }
            Err(e) => {
                let _ = response_tx.send(format!("Request failed: {e}")).await;
                break;
            }
        }
    }
}

fn draw(frame: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(3),    // log area
            Constraint::Length(3), // input area
        ])
        .split(frame.area());

    // Log area — show most recent entries that fit.
    let log_height = chunks[0].height.saturating_sub(2) as usize;
    let start = app.log.len().saturating_sub(log_height);
    let items: Vec<ListItem> = app.log[start..]
        .iter()
        .map(|s| {
            let style = if s.starts_with("FILL:") {
                Style::default().fg(Color::Green)
            } else if s.starts_with("REJECTED:") || s.starts_with("ENGINE ERROR") {
                Style::default().fg(Color::Red)
            } else if s.starts_with("PLACED:") {
                Style::default().fg(Color::Cyan)
            } else if s.starts_with("CANCELLED:") {
                Style::default().fg(Color::Yellow)
            } else if s.starts_with('>') {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default()
            };
            ListItem::new(Line::from(Span::styled(s.as_str(), style)))
        })
        .collect();

    let log_list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Trading TUI "),
    );
    frame.render_widget(log_list, chunks[0]);

    // Input area.
    let input = Paragraph::new(app.input.as_str())
        .style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .block(Block::default().borders(Borders::ALL).title(" Command "));
    frame.render_widget(input, chunks[1]);

    // Place cursor at end of input.
    frame.set_cursor_position((chunks[1].x + app.input.len() as u16 + 1, chunks[1].y + 1));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9876".into())
        .parse()?;

    // Channels between TUI and client task.
    // Small capacity — TUI is human-speed, no need for large buffers.
    let (request_tx, request_rx) = mpsc::channel::<Request>(16);
    let (response_tx, response_rx) = mpsc::channel::<String>(64);

    // Spawn the client task.
    tokio::spawn(client_task(addr, request_rx, response_tx));

    // Initialize terminal.
    let mut terminal = ratatui::init();

    let mut app = App::new(request_tx, response_rx);

    loop {
        app.poll_responses();
        terminal.draw(|f| draw(f, &app))?;

        if app.quit {
            break;
        }

        // Poll for input with a short timeout so we can also poll responses.
        if event::poll(std::time::Duration::from_millis(50))?
            && let Event::Key(key) = event::read()?
        {
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match key.code {
                KeyCode::Enter => app.submit_command(),
                KeyCode::Char(c) => app.input.push(c),
                KeyCode::Backspace => {
                    app.input.pop();
                }
                KeyCode::Esc => app.quit = true,
                _ => {}
            }
        }
    }

    ratatui::restore();
    Ok(())
}
