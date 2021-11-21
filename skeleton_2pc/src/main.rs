#[macro_use]
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
extern crate ipc_channel;
use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::process::{Child,Command};
use ipc_channel::ipc::IpcSender as Sender;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::channel;
pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
use message::ProtocolMessage;
use std::thread;
use client::Client;
use participant::Participant;
use std::{time::Duration};

///
/// pub fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     child_opts: CLI options for child process
///
/// 1. Set up IPC
/// 2. Spawn a child process using the child CLI options
/// 3. Do any required communication to set up the parent / child communication channels
/// 4. Return the child process handle and the communication channels for the parent
///
/// HINT: You can change the signature of the function if necessary
///
fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let (server, server_name) = IpcOneShotServer::<(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>::new().unwrap();
    println!("{}",server_name.to_string());
    child_opts.ipc_path = server_name.clone();
    //println!("{}",child_opts.ipc_path.to_string());

    let child = Command::new(env::current_exe().unwrap())
        .args(child_opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");

    //let (tx, rx):(Sender::<ProtocolMessage>, Receiver::<ProtocolMessage>) = channel().unwrap();
    // TODO

    let (_, (tx,rx)) = server.accept().unwrap();

println!("hello");
    (child, tx, rx )
}

///
/// pub fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     opts: CLI options for this process
///
/// 1. Connect to the parent via IPC
/// 2. Do any required communication to set up the parent / child communication channels
/// 3. Return the communication channels for the child
///
/// HINT: You can change the signature of the function if necessasry
///
fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let (tx, rx) = channel().unwrap();
    
    // TODO

    (tx, rx)
}

///
/// pub fn run(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Creates a new coordinator
/// 2. Spawns and connects to new clients processes and then registers them with
///    the coordinator
/// 3. Spawns and connects to new participant processes and then registers them
///    with the coordinator
/// 4. Starts the coordinator protocol
/// 5. Wait until the children finish execution
///
fn run(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let coord_log_path = format!("{}//{}", opts.log_path, "coordinator.log");
    println!("{}", opts.mode);
    // TODO

    let mut coor = coordinator::Coordinator::new( coord_log_path, &running, opts.num_requests.clone());
    let mut counter = 0;

    loop{
           
        let mut client_opts = opts.clone();
		client_opts.mode = "client".to_string();
		client_opts.num =counter;
		let ( client, coor_cl_tx, cl_coor_rx) = spawn_child_and_connect( &mut client_opts.clone());
		
        let proc_name=client_opts.num.to_string();
		
		println!("{}",proc_name);
        coor.client_join(&proc_name,coor_cl_tx, cl_coor_rx);
				
        counter+= 1;
        if counter == opts.num_clients{
            break;
        }
    }
    counter =0;
    loop{
        let mut part_opts = opts.clone();
		part_opts.mode = "participant".to_string();
		part_opts.num =counter;
		let ( participant, coor_part_tx, part_coor_rx) = spawn_child_and_connect( &mut part_opts.clone());

        let proc_name=part_opts.num.to_string();
        coor.participant_join(&proc_name, coor_part_tx, part_coor_rx);

        counter+= 1;
        if counter == opts.num_participants{
            break;
        }
    };
    

}

///
/// pub fn run_client(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new client
/// 3. Starts the client protocol
///
fn run_client(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    // TODO
    println!("in client");
	let server = Sender::connect(opts.ipc_path.clone()).unwrap();
    let (cl_coor_tx, cl_coor_rx):(Sender::<ProtocolMessage>, Receiver::<ProtocolMessage>) = channel().unwrap();
	let (coor_cl_tx, coor_cl_rx):(Sender::<ProtocolMessage>, Receiver::<ProtocolMessage>) = channel().unwrap();
	server.send((coor_cl_tx,cl_coor_rx)).unwrap();
	
	let mut client = Client::new(opts.num.to_string(),  running,  cl_coor_tx,coor_cl_rx);
	client.protocol(opts.num_requests.clone());
	
}

///
/// pub fn run_participant(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new participant
/// 3. Starts the participant protocol
///
fn run_participant(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let participant_id_str = format!("participant_{}", opts.num);
    let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str);

    // TODO
	println!("in participant");
	let server = Sender::connect(opts.ipc_path.clone()).unwrap();
    let (part_coor_tx, part_coor_rx):(Sender::<ProtocolMessage>, Receiver::<ProtocolMessage>) = channel().unwrap();
	let (coor_part_tx, coor_part_rx):(Sender::<ProtocolMessage>, Receiver::<ProtocolMessage>) = channel().unwrap();
	server.send((coor_part_tx,part_coor_rx)).unwrap();
	
	
	let mut participant = Participant::new( opts.num.to_string(), opts.log_path.clone(), running,opts.send_success_probability, opts.operation_success_probability,part_coor_tx,coor_part_rx);
	participant.protocol();
}

fn main() {
    // Parse CLI arguments
    let opts = tpcoptions::TPCOptions::new();
    // Set-up logging and create OpLog path if necessary
    stderrlog::new()
            .module(module_path!())
            .quiet(false)
            .timestamp(stderrlog::Timestamp::Millisecond)
            .verbosity(opts.verbosity)
            .init()
            .unwrap();
    match fs::create_dir_all(opts.log_path.clone()) {
        Err(e) => error!("Failed to create log_path: \"{:?}\". Error \"{:?}\"", opts.log_path, e),
        _ => (),
    }

    // Set-up Ctrl-C / SIGINT handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = opts.mode.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        if m == "run" {
            print!("\n");
        }
    }).expect("Error setting signal handler!");

    // Execute main logic
    match opts.mode.as_ref() {
        "run" => run(&opts, running),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => checker::check_last_run(opts.num_clients, opts.num_requests, opts.num_participants, &opts.log_path),
        _ => panic!("Unknown mode"),
    }
}
