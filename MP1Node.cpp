/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */

	return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {

		
	Address addr;
	int id;
	short port; 
	long hb;
	// Get the message type, and switch on the message type
	MessageHdr* m = (MessageHdr*)data;
	MessageHdr* mOut;
	switch(m->msgType){
		case JOINREQ:
		{
			cout << "JOINREQ recieved" << endl;
			// Get address and heartbeat of sender from message
			memcpy(&addr, (char*)(m+1), 					sizeof(addr));
			memcpy(&hb,   (char*)(m+1) + 1 + sizeof(addr),  sizeof(long));
			// Get id and port from address (address = id:port)
			id = *((int*)addr.addr);
			port = *( (short*)(&addr.addr[4]) );
			cout << "JOINREQ from id = " << id << " and port = " << port << endl;
			// Update membership list of introducer with this new node
			MemberListEntry newNode(id, port, hb, par->getcurrtime()); 
			// The member list will be indexed by ID
			memberNode->memberList.push_back(newNode);	

			int numNodes = memberNode->memberList.size();
			// Create JOINREP message
			size_t msgSize = sizeof(MessageHdr) + sizeof(long)*4*numNodes;
			// In the message, include list of known nodes
			mOut = (MessageHdr*)malloc(msgSize);
			mOut->msgType = JOINREP;
			auto it = memberNode->memberList.begin();
			long* nodeData  = (long*)(mOut + 1);
			cout << "Introducer sending info for nodes "; 
			for(int c = 0 ; it !=memberNode->memberList.end() ; it++){
				nodeData[c++] = (long)it->id;
				nodeData[c++] = (long)it->port;
				nodeData[c++] = it->heartbeat;
				nodeData[c++] = it->timestamp;
				cout << it->id << " ";
			}
			cout << endl;
			// send the JOINREP message to the node that send JOINREQ
			emulNet->ENsend(&memberNode->addr, &addr, (char*)mOut, msgSize);
			
			//clean up memory
			free(mOut);

			break;
		}	
		case JOINREP:
		{
			cout << "JOINTREP recieved" << endl;
			long* nodeData = (long*)(m + 1);
			int numNodes = (size - sizeof(MessageHdr))/(4*sizeof(long));
			cout << "numNodes = " << numNodes << endl;
			int id;
			short port;
			long hb, ts;
			// Create a vector from the message recieved from introducer
			cout << "got info for nodes ";
			for(int i = 0; i < numNodes*4; ){
				id   = (int)nodeData[i++];
				port = (short)nodeData[i++];
				hb   = nodeData[i++];
				ts   = nodeData[i++];
				cout << id << " ";
				MemberListEntry mle(id, port, hb, ts);
				memberNode->memberList.push_back(mle);
			}
			cout << endl;
			break;
		}
		case PING:
		{
			cout << "PING recieved" << endl;
			memcpy(&addr, (char*)(m+1), sizeof(addr));
			// Update membership list based on message in data

			// Send an ACK message back :: msgType:senderAddress
			size_t msgSize = sizeof(MessageHdr) + sizeof(Address);
			mOut = (MessageHdr*)malloc(msgSize);
			mOut->msgType = ACK;
			memcpy((char *)(mOut+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
			free(mOut);
			break;
		}
		case ACK:
		{
			cout << "ACK recieved" << endl;
			memcpy(&addr, (char*)(m+1), sizeof(addr));
			//Remove the process from the pingList
			string strAddr(addr.addr);
			pingMap[strAddr] = NOT_PINGED;
			break;
		}
		case IPING:
		{
			cout << "INDIRECT PING recieved" << endl;
			break;
		}
		case IACK:
		{
			cout << "INDIRECT ACK recieved" << endl;
			break;
		}
		default:
			cout << "INVALID MESSAGE" << endl;
			break;
	}
	
	return true;	
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
	int currTime = par->getcurrtime();
	MessageHdr* mOut;
	// Check list of processes that have been sent a PING
	// If the process has timed out, send IPING to K processes and refresh the timer 
	map<string, long>::iterator itMap;
	for(itMap = pingMap.begin(); itMap != pingMap.end(); itMap++){
		if(itMap->second != NOT_PINGED && currTime - itMap->second  > TFAIL){
			cout << "Process with addr " << itMap->first << " failed" << endl;
		}	
	}	

	// Construct PING message :: msgType:senderAddress:nodeVectorData
	int numNodes = memberNode->memberList.size();
	size_t msgSize = sizeof(MessageHdr) + sizeof(Address) + sizeof(long)*4*numNodes;
	// In the message, include list of known nodes
	mOut = (MessageHdr*)malloc(msgSize);
	mOut->msgType = PING;
    memcpy((char *)(mOut+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
	auto it = memberNode->memberList.begin();
	long* nodeData  = (long*)((char*)(mOut + 1) + sizeof(Address));
	for(int c = 0 ; it !=memberNode->memberList.end() ; it++){
		nodeData[c++] = (long)it->id;
		nodeData[c++] = (long)it->port;
		nodeData[c++] = it->heartbeat;
		nodeData[c++] = it->timestamp;
	}
	// Include process list on PING message
	// Chose M random processes to send a PING
	Address addr;
	int id;
	short port;
	//TODO: Cannot ping yourself 
	for(int i = 0; i < M; i++){
		int p = rand() % memberNode->memberList.size();
		port = memberNode->memberList[p].port;
		id   = memberNode->memberList[p].id;
		memcpy(&addr.addr[0], &id,   sizeof(int));
		memcpy(&addr.addr[4], &port, sizeof(short));

		//TODO: Ping only if the process is not already in the ping list
		emulNet->ENsend(&memberNode->addr, &addr, (char*)mOut, msgSize);
		// Add processes to PING map
		std::pair<string, long> pme(addr.getAddress(), currTime); //ping Map Entry
		pingMap.insert(pme);
		cout << "Pinged process with address " << addr.getAddress() << endl;
	}

	//clean up memory
	free(mOut);
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
