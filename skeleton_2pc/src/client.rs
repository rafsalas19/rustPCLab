//!
//! client.rs
//! Implementation of 2PC client
//!
extern crate ipc_channel;
extern crate log;
extern crate stderrlog;

use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;

use client::ipc_channel::ipc::IpcReceiver as Receiver;
use client::ipc_channel::ipc::TryRecvError;
use client::ipc_channel::ipc::IpcSender as Sender;

use message;
use message::MessageType;
use message::RequestStatus;
// Client state and primitives for communicating with the coordinator
#[derive(Debug)]
pub struct Client {
    pub id_str: String,
    pub running: Arc<AtomicBool>,
    pub num_requests: u32,
	tx : Sender<message::ProtocolMessage>,
	rx : Receiver<message::ProtocolMessage>,
	successful_ops: u64,
    failed_ops: u64,
    unknown_ops: u64,
}

///
/// Client Implementation
/// Required:
/// 1. new -- constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown
/// 3. pub fn protocol(&mut self, n_requests: i32) -- Implements client side protocol
///
impl Client {

    ///
    /// new()
    ///
    /// Constructs and returns a new client, ready to run the 2PC protocol
    /// with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->client and client->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor
    ///
    pub fn new(id_str: String,
               running: Arc<AtomicBool>,
			   sender: Sender<message::ProtocolMessage>,
				recvr: Receiver<message::ProtocolMessage>
			   ) -> Client {
        Client {
            id_str: id_str,
            running: running,
            num_requests: 0,
			tx: sender,
			rx: recvr,
			successful_ops: 0,
			failed_ops: 0,
			unknown_ops: 0,
					
            // TODO
        }
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());
        match self.rx.recv() {
			Ok(res) => { // Do something interesting wth your result
				if res.mtype==MessageType::CoordinatorExit &&  res.txid == "done" {
					      trace!("{}::Exiting", self.id_str.clone());
				}
            },
            Err(_) => {
                      trace!("{}::Exiting", self.id_str.clone());
            }
        }
		
  
    }

    ///
    /// send_next_operation(&mut self)
    /// Send the next operation to the coordinator
    ///
    pub fn send_next_operation(&mut self) {

        // Create a new request with a unique TXID.
        self.num_requests = self.num_requests + 1;
        let txid = format!("client{}_op_{}", self.id_str.clone(), self.num_requests);
		let cl_id: u32 = self.id_str.parse().unwrap();
        let pm = message::ProtocolMessage::generate(message::MessageType::ClientRequest,
                                                    txid.clone(),
                                                    self.id_str.clone(),
                                                    self.num_requests,
													cl_id.clone());
        info!("{}::Sending operation #{}", self.id_str.clone(), self.num_requests);

        // TODO
		self.tx.send(pm).unwrap();

        trace!("{}::Sent operation #{}", self.id_str.clone(), self.num_requests);
    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the
    /// last issued request. Note that we assume the coordinator does
    /// not fail in this simulation
    ///
    pub fn recv_result(&mut self)-> bool {

        info!("{}::Receiving Coordinator Result", self.id_str.clone());

		match self.rx.recv() {
			Ok(res) => { // Do something interesting wth your result
				if res.mtype==MessageType::CoordinatorExit &&  res.txid == "done" {
					return false	
				}
				match res.mtype{
					MessageType::ClientResultCommit => self.successful_ops+=1,
					MessageType::ClientResultAbort => self.failed_ops+=1,   
					//CoordinatorExit => self.running =false,
					_ => println!("not supposed to be here")
				}
            },
            Err(_) => {

            }
        }
		true
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this client before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        let successful_ops: u64 = self.successful_ops;
        let failed_ops: u64 = self.failed_ops;
        let unknown_ops: u64 = 0;

        println!("{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", format!("client_{}",self.id_str.clone()), successful_ops, failed_ops, unknown_ops);
    }

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    ///
    pub fn protocol(&mut self, n_requests: u32) {

       // println!("client protocol{}",self.id_str);
		let mut counter =0;
		loop{
			self.send_next_operation();
			counter+= 1;
			if counter == n_requests{
				break;
			}
		}
		counter =0;
		
		loop{
			let success = self.recv_result();
			
			counter+= 1;
			if counter == n_requests || !success{
				break;
			}
		}
		//println!("received results {}",self.id_str);
        self.wait_for_exit_signal();
        self.report_status();
    }
}
