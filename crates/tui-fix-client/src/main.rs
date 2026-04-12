//! TUI trading client that speaks FIX 4.4 to both the oe-gateway and md-gateway.
//!
//! Usage:
//!   melin-tui-fix-client --oe-addr 127.0.0.1:9000 --md-addr 127.0.0.1:9001 \
//!     --sender CLIENT --oe-target MELIN-OE --md-target MELIN-MD

pub mod fix_client;

use std::io;
use std::net::SocketAddr;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use fix_client::FixClient;
use melin_gateway_core::fix::parse::{Field, FixMessage};
use melin_gateway_core::fix::serialize::FixMessageBuilder;
use melin_gateway_core::fix::tags;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table};

/// (price, size, order_count) for one book level.
type BookLevel = (String, String, String);

enum UiMsg {
    MdStatus(bool, String),
    Book(Vec<BookLevel>, Vec<BookLevel>),
    OeStatus(bool, String),
    ActiveOrders(Vec<String>),
    Balances(Vec<String>),
}

struct App {
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    active_orders: Vec<String>,
    balances: Vec<String>,
    status: String,
    md_ok: bool,
    oe_ok: bool,
}

impl App {
    fn new(status: String) -> Self {
        Self {
            bids: vec![],
            asks: vec![],
            active_orders: vec![],
            balances: vec![],
            status,
            md_ok: false,
            oe_ok: false,
        }
    }
    fn drain(&mut self, rx: &Receiver<UiMsg>) {
        while let Ok(m) = rx.try_recv() {
            match m {
                UiMsg::MdStatus(ok, s) => {
                    self.md_ok = ok;
                    if !s.is_empty() {
                        self.status = s;
                    }
                }
                UiMsg::Book(b, a) => {
                    self.bids = b;
                    self.asks = a;
                }
                UiMsg::OeStatus(ok, s) => {
                    self.oe_ok = ok;
                    if !s.is_empty() {
                        self.status = s;
                    }
                }
                UiMsg::ActiveOrders(o) => self.active_orders = o,
                UiMsg::Balances(b) => self.balances = b,
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let (mut oe_addr, mut md_addr) = ("127.0.0.1:9000".into(), "127.0.0.1:9001".into());
    let (mut sender, mut oe_target, mut md_target) =
        ("CLIENT".into(), "MELIN-OE".into(), "MELIN-MD".into());
    let mut i = 1;
    while i < args.len() {
        let val = || args.get(i + 1).cloned().unwrap_or_default();
        match args[i].as_str() {
            "--oe-addr" => {
                oe_addr = val();
                i += 1;
            }
            "--md-addr" => {
                md_addr = val();
                i += 1;
            }
            "--sender" => {
                sender = val();
                i += 1;
            }
            "--oe-target" => {
                oe_target = val();
                i += 1;
            }
            "--md-target" => {
                md_target = val();
                i += 1;
            }
            _ => {
                eprintln!(
                    "usage: melin-tui-fix-client [--oe-addr ADDR] [--md-addr ADDR] [--sender ID] [--oe-target ID] [--md-target ID]"
                );
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let (tx, rx) = mpsc::channel::<UiMsg>();
    let (md_tx, md_a, md_t, md_s) = (
        tx.clone(),
        md_addr.clone(),
        md_target.clone(),
        sender.clone(),
    );
    thread::spawn(move || run_md_session(&md_a, &md_s, &md_t, &md_tx));
    let (oe_tx, oe_a, oe_t, oe_s) = (tx, oe_addr.clone(), oe_target.clone(), sender.clone());
    thread::spawn(move || run_oe_session(&oe_a, &oe_s, &oe_t, &oe_tx));

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;
    let mut app = App::new(format!(
        "OE: {oe_addr} -> {oe_target}  |  MD: {md_addr} -> {md_target}  |  'q' to quit"
    ));

    loop {
        app.drain(&rx);
        terminal.draw(|f| {
            let v = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Min(5),
                    Constraint::Min(5),
                    Constraint::Length(3),
                ])
                .split(f.area());
            let top = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(v[0]);
            let bot = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(v[1]);

            let (mi, oi) = (icon(app.md_ok), icon(app.oe_ok));

            // Book: asks reversed (best at bottom), separator, bids.
            let mut rows: Vec<Row> = Vec::new();
            for a in app.asks.iter().rev() {
                rows.push(
                    Row::new(vec!["", "", "", a.0.as_str(), a.1.as_str(), a.2.as_str()])
                        .style(Style::default().fg(Color::Red)),
                );
            }
            rows.push(Row::new(vec!["---", "", "", "---", "", ""]));
            for b in &app.bids {
                rows.push(
                    Row::new(vec![b.0.as_str(), b.1.as_str(), b.2.as_str(), "", "", ""])
                        .style(Style::default().fg(Color::Green)),
                );
            }
            let widths = [
                Constraint::Percentage(20),
                Constraint::Percentage(15),
                Constraint::Percentage(10),
                Constraint::Percentage(20),
                Constraint::Percentage(15),
                Constraint::Percentage(10),
            ];
            f.render_widget(
                Table::new(rows, widths)
                    .header(
                        Row::new(vec!["Bid Px", "Size", "#", "Ask Px", "Size", "#"])
                            .style(Style::default().add_modifier(Modifier::BOLD)),
                    )
                    .block(
                        Block::default()
                            .title(format!(" Order Book {mi} "))
                            .borders(Borders::ALL),
                    ),
                top[0],
            );

            f.render_widget(
                panel(&format!(" Balances {oi} "), &app.balances, "(waiting)"),
                top[1],
            );
            f.render_widget(
                panel(
                    &format!(" Active Orders {oi} "),
                    &app.active_orders,
                    "(none)",
                ),
                bot[0],
            );
            f.render_widget(
                Paragraph::new(vec![Line::from("  (not yet implemented)")]).block(
                    Block::default()
                        .title(format!(" Recent Trades {mi} "))
                        .borders(Borders::ALL),
                ),
                bot[1],
            );
            f.render_widget(
                Paragraph::new(Line::from(app.status.as_str()))
                    .style(Style::default().fg(Color::Cyan))
                    .block(Block::default().borders(Borders::TOP)),
                v[2],
            );
        })?;

        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
            && key.code == KeyCode::Char('q')
        {
            break;
        }
    }
    disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

fn icon(ok: bool) -> &'static str {
    if ok { "[+]" } else { "[-]" }
}

fn panel<'a>(title: &'a str, items: &'a [String], empty: &'a str) -> Paragraph<'a> {
    let lines: Vec<Line> = if items.is_empty() {
        vec![Line::from(format!("  {empty}"))]
    } else {
        items.iter().map(|s| Line::from(format!("  {s}"))).collect()
    };
    Paragraph::new(lines).block(
        Block::default()
            .title(title.to_string())
            .borders(Borders::ALL),
    )
}

// --- MD session ---------------------------------------------------------------

fn run_md_session(addr: &str, sender: &str, target: &str, tx: &Sender<UiMsg>) {
    let send_err = |e: &dyn std::fmt::Display| {
        let _ = tx.send(UiMsg::MdStatus(false, format!("MD: {e}")));
    };
    let sock: SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(e) => {
            send_err(&e);
            return;
        }
    };
    let mut c = match FixClient::connect(sock, sender, target, 30) {
        Ok(c) => c,
        Err(e) => {
            send_err(&e);
            return;
        }
    };
    let _ = tx.send(UiMsg::MdStatus(true, String::new()));

    // Discover symbols via SecurityListRequest.
    let slr = FixMessageBuilder::new(tags::MSG_SECURITY_LIST_REQUEST)
        .str_tag(tags::SECURITY_REQ_ID, "SLR1")
        .str_tag(tags::SECURITY_LIST_REQUEST_TYPE, "0");
    if let Err(e) = c.send_builder(slr) {
        send_err(&e);
        return;
    }

    let symbols: Vec<String> = match c.recv() {
        Ok(msg) if msg.msg_type() == tags::MSG_SECURITY_LIST => msg
            .fields_iter()
            .filter(|f| f.tag == tags::SYMBOL)
            .filter_map(|f| std::str::from_utf8(f.value).ok())
            .map(String::from)
            .collect(),
        Ok(_) => vec![],
        Err(e) => {
            send_err(&e);
            return;
        }
    };

    // Subscribe to each symbol.
    for sym in &symbols {
        let mdr = FixMessageBuilder::new(tags::MSG_MARKET_DATA_REQUEST)
            .str_tag(tags::MD_REQ_ID, &format!("MD-{sym}"))
            .str_tag(tags::SUBSCRIPTION_REQUEST_TYPE, "1")
            .str_tag(tags::MARKET_DEPTH, "0")
            .str_tag(tags::MD_UPDATE_TYPE, "0")
            .u64_tag(tags::NO_RELATED_SYM, 1)
            .str_tag(tags::SYMBOL, sym);
        if let Err(e) = c.send_builder(mdr) {
            send_err(&e);
            return;
        }
    }

    if let Err(e) = c.set_read_timeout(Some(Duration::from_millis(100))) {
        send_err(&e);
        return;
    }
    loop {
        match c.try_recv() {
            Ok(Some(msg)) if msg.msg_type() == tags::MSG_MD_SNAPSHOT => {
                let (b, a) = parse_snapshot(&msg);
                if tx.send(UiMsg::Book(b, a)).is_err() {
                    return;
                }
            }
            Ok(_) => {}
            Err(e) => {
                send_err(&e);
                return;
            }
        }
    }
}

/// Extract bid/ask levels from a W (snapshot) message.
fn parse_snapshot(msg: &FixMessage<'_>) -> (Vec<BookLevel>, Vec<BookLevel>) {
    let (mut bids, mut asks) = (Vec::new(), Vec::new());
    let fields: Vec<&Field<'_>> = msg.fields_iter().collect();
    let mut i = 0;
    while i < fields.len() {
        if fields[i].tag == tags::MD_ENTRY_TYPE {
            let et = fields[i].value;
            let val = |off, tag: u32| {
                fields
                    .get(i + off)
                    .filter(|f: &&&Field<'_>| f.tag == tag)
                    .and_then(|f| std::str::from_utf8(f.value).ok())
                    .unwrap_or("-")
                    .to_string()
            };
            let lev = (
                val(1, tags::MD_ENTRY_PX),
                val(2, tags::MD_ENTRY_SIZE),
                val(3, tags::NUMBER_OF_ORDERS),
            );
            match et {
                b"0" => bids.push(lev),
                b"1" => asks.push(lev),
                _ => {}
            }
            i += 4;
        } else {
            i += 1;
        }
    }
    (bids, asks)
}

// --- OE session ---------------------------------------------------------------

fn run_oe_session(addr: &str, sender: &str, target: &str, tx: &Sender<UiMsg>) {
    let send_err = |e: &dyn std::fmt::Display| {
        let _ = tx.send(UiMsg::OeStatus(false, format!("OE: {e}")));
    };
    let sock: SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(e) => {
            send_err(&e);
            return;
        }
    };
    let mut c = match FixClient::connect(sock, sender, target, 30) {
        Ok(c) => c,
        Err(e) => {
            send_err(&e);
            return;
        }
    };
    let _ = tx.send(UiMsg::OeStatus(true, String::new()));
    if let Err(e) = c.set_read_timeout(Some(Duration::from_millis(100))) {
        send_err(&e);
        return;
    }

    let (mut last_q, mut msr_n, mut pr_n) = (Instant::now() - Duration::from_secs(10), 0u64, 0u64);
    // Accumulate mass-status reports until LastRptRequested=Y.
    let mut pending_orders: Vec<String> = Vec::new();
    loop {
        if last_q.elapsed() >= Duration::from_secs(2) {
            msr_n += 1;
            let msr = FixMessageBuilder::new(tags::MSG_ORDER_MASS_STATUS_REQUEST)
                .str_tag(tags::MASS_STATUS_REQ_ID, &format!("MSR{msr_n}"))
                .str_tag(tags::MASS_STATUS_REQ_TYPE, "1");
            if let Err(e) = c.send_builder(msr) {
                send_err(&e);
                return;
            }
            pr_n += 1;
            let pr = FixMessageBuilder::new(tags::MSG_REQUEST_FOR_POSITIONS)
                .str_tag(tags::POS_REQ_ID, &format!("PR{pr_n}"))
                .str_tag(tags::POS_REQ_TYPE, "0")
                .str_tag(tags::ACCOUNT, sender);
            if let Err(e) = c.send_builder(pr) {
                send_err(&e);
                return;
            }
            pending_orders.clear();
            last_q = Instant::now();
        }
        match c.try_recv() {
            Ok(Some(msg)) => {
                let mt = msg.msg_type();
                if mt == tags::MSG_EXECUTION_REPORT
                    && msg.get_str(tags::MASS_STATUS_REQ_ID).is_some()
                {
                    // Accumulate individual order reports.
                    if msg.get_str(tags::TOT_NUM_REPORTS) != Some("0") {
                        pending_orders.extend(parse_mass_status(&msg));
                    }
                    // Flush on last report.
                    if msg.get_str(tags::LAST_RPT_REQUESTED) == Some("Y") {
                        if tx
                            .send(UiMsg::ActiveOrders(pending_orders.clone()))
                            .is_err()
                        {
                            return;
                        }
                        pending_orders.clear();
                    }
                } else if mt == tags::MSG_POSITION_REPORT
                    && tx.send(UiMsg::Balances(parse_positions(&msg))).is_err()
                {
                    return;
                }
            }
            Ok(None) => {}
            Err(e) => {
                send_err(&e);
                return;
            }
        }
    }
}

fn parse_mass_status(msg: &FixMessage<'_>) -> Vec<String> {
    if msg.get_str(tags::TOT_NUM_REPORTS) == Some("0") {
        return vec![];
    }
    let g = |t| msg.get_str(t).unwrap_or("-");
    let side = match msg.get_str(tags::SIDE) {
        Some("1") => "BUY",
        Some("2") => "SELL",
        _ => "?",
    };
    vec![format!(
        "{} {} {} {}@{} leaves={} st={}",
        g(tags::ORDER_ID),
        g(tags::SYMBOL),
        side,
        g(tags::ORDER_QTY),
        g(tags::PRICE),
        g(tags::LEAVES_QTY),
        g(tags::ORD_STATUS)
    )]
}

fn parse_positions(msg: &FixMessage<'_>) -> Vec<String> {
    let fields: Vec<&Field<'_>> = msg.fields_iter().collect();
    let (mut out, mut i) = (Vec::new(), 0);
    while i < fields.len() {
        if fields[i].tag == tags::CURRENCY {
            let s = |off, tag: u32| {
                fields
                    .get(i + off)
                    .filter(|f: &&&Field<'_>| f.tag == tag)
                    .and_then(|f| std::str::from_utf8(f.value).ok())
                    .unwrap_or("0")
            };
            let ccy = std::str::from_utf8(fields[i].value).unwrap_or("?");
            out.push(format!(
                "{ccy}: free={}  reserved={}",
                s(1, tags::LONG_QTY),
                s(2, tags::SHORT_QTY)
            ));
            i += 3;
        } else {
            i += 1;
        }
    }
    out
}
