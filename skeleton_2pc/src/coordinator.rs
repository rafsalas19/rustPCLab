//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;


use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
use coordinator::ipc_channel::ipc::channel;

use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedVotesCommit,
    SentGlobalDecision
}

/// Coordinator
/// Struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: oplog::OpLog,
	client_map: HashMap<String,(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
	part_map: HashMap<String,(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
	num_requests: u32,
}

///
/// Coordinator
/// Implementation of coordinator functionality
/// Required:
/// 1. new -- Constructor
/// 2. protocol -- Implementation of coordinator side of protocol
/// 3. report_status -- Report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- What to do when a participant joins
/// 5. client_join -- What to do when a client joins
///
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    ///
    /// <params>
    ///     log_path: directory for log files --> create a new log there.
    ///     r: atomic bool --> still running?
    ///
    pub fn new(
        log_path: String,
        r: &Arc<AtomicBool>, nr: u32) -> Coordinator {

        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r.clone(),
			client_map: HashMap::new(),
			part_map: HashMap::new(),
            num_requests: nr,
        }
    }

    ///
    /// participant_join()
    /// Adds a new participant for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn participant_join(&mut self, name: &String, tx: Sender<ProtocolMessage>,rx: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);
       // TODO
	   	self.part_map.insert( name.clone(),(tx,rx));
    }

    ///
    /// client_join()
    /// Adds a new client for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn client_join(&mut self, name: &String, tx: Sender<ProtocolMessage>,rx: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
		self.client_map.insert( name.clone(),(tx,rx));
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        let successful_ops: u64 = 0;
        let failed_ops: u64 = 0;
        let unknown_ops: u64 = 0;

        println!("coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", successful_ops, failed_ops, unknown_ops);
    }
	pub fn send_result(&mut self,pm: ProtocolMessage, &tx: Sender<ProtocolMessage>){
		
		match pm.mtype{
			MessageType::ParticipantVoteCommit => pm.mtype = MessageType::ClientResultCommit,  
			MessageType::ParticipantVoteAbort => pm.mtype = MessageType::ClientResultAbort, 
			_ =>{
				//nothing
			}								
		}
		coor_cl_tx.send(pm).unwrap()
	}
    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {

        // TODO
		//receive request from client
		let mut request: ProtocolMessage;
		let mut pm_queue: Vec<ProtocolMessage> = Vec::new();
		let mut counter =0;
		loop{
			for (id, val) in &self.client_map {
					let (_, rx)= val;
					match rx.recv() {
					Ok(res) => { 
							request = res.clone();
							pm_queue.push(request);				
					},
					Err(_) => {
						//wait
					}
				}
			}
			counter+=1;
			if counter==self.num_requests {
				break;
			}
		}
		//send request to participants
		let mut msg_in_flight: Vec<(ProtocolMessage,String)>= Vec::new();
		while pm_queue.len() !=0{
			let mut msg = pm_queue[0].clone();	
			msg.mtype = MessageType::CoordinatorPropose;
			//send message to participant
			for (id, val) in &self.part_map{				
				//msg_in_flight.push(msg.clone(),id.clone());			
				let (tx,_)= val;
				tx.send(msg).unwrap();
			}
					
			//recieve messages
			let mut votingblock: Vec<(bool)>= Vec::new();
			loop{
				for (id, val) in &self.part_map{
					let (_, rx) = val;
					match rx.try_recv() {
						Ok(res) => {
							let part_id: u32 = self.id.parse().unwrap();
							
						},
						Err(TryRecvError::Empty) => continue,
						Err(_) =>{// an error occured
						}
					}	
				}
				
			}
			
			pm_queue.remove(0);
			
		}


        self.report_status();
    }
}
