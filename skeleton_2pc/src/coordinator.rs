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
use std::time::Instant;

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
	successful_ops: u64,
    failed_ops: u64,
    unknown_ops: u64,
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
			successful_ops: 0,
			failed_ops: 0,
			unknown_ops: 0,
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
        let successful_ops: u64 = self.successful_ops;
        let failed_ops: u64 = self.failed_ops;
        let unknown_ops: u64 = self.unknown_ops;

        println!("coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", successful_ops, failed_ops, unknown_ops);
    }
	pub fn send_result(&mut self, mut pm:  ProtocolMessage, tx: &Sender<ProtocolMessage>){
		
		match pm.mtype{
			MessageType::ParticipantVoteCommit => pm.mtype = MessageType::ClientResultCommit,  
			MessageType::ParticipantVoteAbort => pm.mtype = MessageType::ClientResultAbort, 
			_ =>{
				//nothing
			}								
		}
		tx.send(pm).unwrap();
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
						//println!("requests{}", res.clone().txid);
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
		self.state = CoordinatorState::ReceivedRequest;
		//send request to participants

		while pm_queue.len() !=0{
			self.state = CoordinatorState::ReceivedRequest;
			let mut commit: bool = true;
			let mut msg = pm_queue[0].clone();	
			msg.mtype = MessageType::CoordinatorPropose;
			//send request to participant
			for (id, val) in &self.part_map{							
				let (tx,_)= val;
				tx.send(msg.clone()).unwrap();
			}
			self.state = CoordinatorState::ProposalSent;
			
			//recieve messages votes
			let mut votingblock: HashMap<u32, RequestStatus> = HashMap::new();
			let timer = Instant::now();
			loop{				
				for (id, val) in &self.part_map{
					let part_id: u32 = id.parse().unwrap();				
					if votingblock.contains_key(&part_id) == true{
						continue;
					}
					let (_, rx) = val;
					match rx.try_recv() {
						Ok(res) => {
							let mtype :MessageType =res.mtype;
							match (mtype) {
								(MessageType::ParticipantVoteCommit) => votingblock.insert(part_id,RequestStatus::Committed),
								(MessageType::ParticipantVoteAbort) => votingblock.insert(part_id,RequestStatus::Aborted),
								_ => votingblock.insert(part_id,RequestStatus::Aborted),
							};
						},
						Err(TryRecvError::Empty) =>{
							let elapsed_time = timer.elapsed();
								if elapsed_time >=  Duration::from_secs(1){
									break;
								}
							//no op;
						},
						Err(_) =>{// an error occured
							votingblock.insert(part_id,RequestStatus::Aborted);
						},
					};	
				}
							
				//timeOut
				let elapsed_time = timer.elapsed();
				if elapsed_time >=  Duration::from_secs(1){
					for (id, val) in &self.part_map{
						let part_id: u32 = id.parse().unwrap();
						if votingblock.contains_key(&part_id) == false{
							votingblock.insert(part_id,RequestStatus::Aborted);
						}				
					}
					break;
				}
				if votingblock.len() == self.part_map.len(){
					break;
				} 
			}
		
			//voting alalysis
			for (id,val) in votingblock{
				if val != RequestStatus::Committed {
					commit = false;
					break;
				}
			}
			//decision
			let id = pm_queue[0].cl_id.to_string();
			let mut pm =pm_queue[0].clone();
			let mut coor_mtype :  MessageType;
			//message to clients
			if commit{
				self.state = CoordinatorState::ReceivedVotesCommit;
				coor_mtype= MessageType::CoordinatorCommit;
				pm.mtype = MessageType::ClientResultCommit; 
				self.successful_ops+=1;
			}
			else{
				self.state = CoordinatorState::ReceivedVotesAbort;
				coor_mtype= MessageType::CoordinatorAbort;
				pm.mtype = MessageType::ClientResultAbort; 
				self.failed_ops+=1;
			}
			let (coor_cl_tx, _) = self.client_map.get(&id).unwrap();
			coor_cl_tx.send(pm.clone()).unwrap();
			pm.mtype = coor_mtype;
			self.log.append( pm.mtype.clone(), pm.txid.clone(), pm.senderid.clone(), pm.opid.clone());
			//Decision to participants
			for (id, val) in &self.part_map{						
				let (tx,_)= val;
				tx.send(pm.clone()).unwrap();
			}
			self.state = CoordinatorState::SentGlobalDecision;
			//request dequeued
			pm_queue.remove(0);
			//Gracefull exit
			
			if !self.running.load(Ordering::SeqCst){
				//participants
				for (id, val) in &self.part_map{						
					let (tx,_)= val;
					let pm = ProtocolMessage::generate( MessageType::CoordinatorExit,"done".to_string(),"done".to_string(),0,0);//t: MessageType, tid: String, sid: String, oid: u32,cid: u32
					tx.send(pm.clone()).unwrap();
				}
				for (id, val) in &self.client_map{						
					let (tx,_)= val;
					let pm = ProtocolMessage::generate( MessageType::CoordinatorExit,"done".to_string(),"done".to_string(),0,0);//t: MessageType, tid: String, sid: String, oid: u32,cid: u32
					tx.send(pm.clone()).unwrap();
				}
				self.report_status();
				return;
			}
						
		}


        self.report_status();
		//exit now
		//tell participants it time to shut down
		println!("ending coor");
		for (id, val) in &self.part_map{						
				let (tx,_)= val;
				let pm = ProtocolMessage::generate( MessageType::CoordinatorExit,"done".to_string(),"done".to_string(),0,0);//t: MessageType, tid: String, sid: String, oid: u32,cid: u32
				tx.send(pm.clone()).unwrap();
		}
		for (id, val) in &self.client_map{						
			let (tx,_)= val;
			let pm = ProtocolMessage::generate( MessageType::CoordinatorExit,"done".to_string(),"done".to_string(),0,0);//t: MessageType, tid: String, sid: String, oid: u32,cid: u32
			tx.send(pm.clone()).unwrap();
		}
		
    }
}
