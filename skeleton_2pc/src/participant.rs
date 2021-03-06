//!
//! participant.rs
//! Implementation of 2PC participant
//!
extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::thread;

use participant::rand::prelude::*;
use participant::ipc_channel::ipc::IpcReceiver as Receiver;
use participant::ipc_channel::ipc::TryRecvError;
use participant::ipc_channel::ipc::IpcSender as Sender;


use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

///
/// ParticipantState
/// enum for Participant 2PC state machine
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {
    Quiescent,
    ReceivedP1,
    VotedAbort,
    VotedCommit,
    AwaitingGlobalDecision,
}

///
/// Participant
/// Structure for maintaining per-participant state and communication/synchronization objects to/from coordinator
///
#[derive(Debug)]
pub struct Participant {
    id_str: String,
    state: ParticipantState,
    log: oplog::OpLog,
    running: Arc<AtomicBool>,
    send_success_prob: f64,
    operation_success_prob: f64,
	tx : Sender<message::ProtocolMessage>,
	rx : Receiver<message::ProtocolMessage>,
	successful_ops: u64,
    failed_ops: u64,
    unknown_ops: u64,
}

///
/// Participant
/// Implementation of participant for the 2PC protocol
/// Required:
/// 1. new -- Constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- Implements participant side protocol for 2PC
///
impl Participant {

    ///
    /// new()
    ///
    /// Return a new participant, ready to run the 2PC protocol with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course.
    ///
    pub fn new(
        id_str: String,
        log_path: String,
        r: Arc<AtomicBool>,
        send_success_prob: f64,
        operation_success_prob: f64,
		sender: Sender<message::ProtocolMessage>,
		recvr: Receiver<message::ProtocolMessage>
		) -> Participant {

        Participant {
            id_str: id_str,
            state: ParticipantState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r,
            send_success_prob: send_success_prob,
            operation_success_prob: operation_success_prob,
			tx: sender,
			rx: recvr,
			successful_ops: 0,
			failed_ops: 0,
			unknown_ops: 0,
            // TODO
        }
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator. This can fail depending on
    /// the success probability. For testing purposes, make sure to not specify
    /// the -S flag so the default value of 1 is used for failproof sending.
    ///
    /// HINT: You will need to implement the actual sending
    ///
    pub fn send(&mut self, pm: ProtocolMessage) {
        let x: f64 = random();
      //  if x <= self.send_success_prob {
            // TODO: Send success
			self.tx.send(pm).unwrap();
		
			
      //  } 
		//else {
            // just do not retry
			//println!("comm faile on p{}", self.id_str.clone());
			
      //  }
    }

    ///
    /// perform_operation
    /// Perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the
    /// command-line option success_probability.
    ///
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic.
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than
    ///       bool if it's more convenient for your design).
    ///
    pub fn perform_operation(&mut self, request_option: &Option<ProtocolMessage>) -> bool {

        trace!("{}::Performing operation", self.id_str.clone());
        let x: f64 = random();
        if x <= self.operation_success_prob {
            // TODO: Successful operation
			self.successful_ops+=1;
			self.state=ParticipantState::VotedCommit;
        } else {
            // TODO: Failed operation
			self.failed_ops+=1;
			self.state=ParticipantState::VotedAbort;
        }

        true
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

        println!("{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", format!("particpant_{}",self.id_str.clone()), successful_ops, failed_ops, unknown_ops);
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        // TODO

        trace!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {

        trace!("{}::Beginning protocol", self.id_str.clone());
		let mut kill: bool;
        // voting
		//let mut counter = 0;
		loop{
			//recvr
			match self.rx.recv() {
				Ok(res) => { 
					self.state=ParticipantState::ReceivedP1;
					let mut request = res.clone();
					if request.mtype==MessageType::CoordinatorExit &&  request.txid == "done" {
						trace!("{}::Exiting", self.id_str.clone());
						//println!("exit!");
						break;
					}
					let optional = None;
					self.perform_operation(&optional); //vote commit or not
					
					match self.state{
						ParticipantState::VotedCommit => {
							request.mtype = MessageType::ParticipantVoteCommit;
							self.state = ParticipantState::VotedCommit;
						},
						_ => {
							request.mtype = MessageType::ParticipantVoteAbort;
							self.state=ParticipantState::VotedAbort;
						}
					}
					self.log.append( request.mtype.clone(), request.txid.clone(), request.senderid.clone(), request.opid.clone());
					self.send(request);
					
				},
				Err(_) => {

				}

			}
			/*counter +=1;
				if counter ==2{
					break;
				}*/
			//println!("voted p{}",self.id_str);
			//await decision
			self.state = ParticipantState::AwaitingGlobalDecision;	
			
			match self.rx.recv() {
				Ok(res) => { 
					let mut request = res.clone();
					self.log.append( request.mtype.clone(), request.txid.clone(), request.senderid.clone(), request.opid.clone());
				},
				Err(_) => {
				}		
			}
			//println!("decision part {}",self.id_str);
			self.state = ParticipantState::Quiescent;				
		}
	    //self.wait_for_exit_signal();
		
		
	
		
        self.report_status();
    }
}
