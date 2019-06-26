
/**********************************
 * FILE NAME: MP1Node.cpp
 * 
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <list>

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
		writeDeltaBuff(memberNode->addr.getAddress(), JOINED);
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));

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

Address MP1Node::processJOINREQ(MessageHdr* mIn){
	Address addr;

	// Get address of sender from message
	memcpy(&addr, (char*)(mIn+1), 					sizeof(addr));
 
	// Update delta buffer and membership map with a new process
	writeDeltaBuff(addr, JOINED);

	return addr;
}

char* MP1Node::createJOINREP(size_t* msgSize){

	int numNodes = memberMap.size();
	*msgSize = sizeof(MessageHdr) + sizeof(long)*2*numNodes;
	// In the message, include list of known nodes
	MessageHdr* mOut = (MessageHdr*)malloc(*msgSize);
	mOut->msgType = JOINREP;
	auto it = memberMap.begin();
	long* nodeData  = (long*)(mOut + 1);
	for(int c = 0 ; it != memberMap.end() ; it++){
		size_t pos = it->first.find(":");
		int id = stoi(it->first.substr(0, pos));
		short port = (short)stoi(it->first.substr(pos + 1, it->first.size()-pos-1));
		nodeData[c++] = (long)id;
		nodeData[c++] = (long)port;
	}
	return (char*)mOut;

}

void MP1Node::processJOINREP(MessageHdr* mIn, int size){

	long* data = (long*)(mIn + 1);
	int numNodes = (size - sizeof(MessageHdr))/(2*sizeof(long));
	int id;
	short port;
	Address addr;
	// Create an empty entry to be added for various messages
	nodeData newEntry = nodeData(NOT_SUSPECTED, NOT_PINGED, 0);
	// Create a vector from the message recieved from introducer
	for(int i = 0; i < numNodes*2; ){
		id   = (int)data[i++];
		port = (short)data[i++];
	
		// Create an Address so it can be added to the grading log
		memcpy(&addr.addr[0], &id,   sizeof(int));
		memcpy(&addr.addr[4], &port, sizeof(short));
		memberMap[addr.getAddress()] = newEntry;
		log->logNodeAdd(&memberNode->addr, &addr);
	}
	// Mark yourself as in the group
	memberNode->inGroup = true;

	// Init timer
	dbTimer = 0;	

	// Tell everyone you've joined!
	writeDeltaBuff(memberNode->addr.getAddress(), JOINED);	
	
}

Address MP1Node::processMessage(char* mIn){
	Address sender;
	Address db_addr;
	dbTypes type;
	// Skip over message type
	mIn += sizeof(MessageHdr);

	// Read sender address
	memcpy(&sender, mIn, sizeof(Address));
	mIn += sizeof(Address);

	// Read delta buffer type
	type = *((dbTypes*)mIn);
	mIn += sizeof(dbTypes);

	// Read delta buffer address
	memcpy(&db_addr, mIn, sizeof(Address));

	// Update membership list based on message in data
	writeDeltaBuff(db_addr, type);	

	return sender;
}

char* MP1Node::createMessage(MsgTypes msgType){

	MessageHdr* mOut;
	// Message Structure: messageType::senderAddress:fail/join byte:failed/joinedAddress 

	// Grab an address from recent change buffer 
	dbTypes db_type;
	Address addr = readDeltaBuff(&db_type);	

	mOut = (MessageHdr*)malloc(SMALL_MSG_SIZE);
	char* mTemp   = (char*)mOut;
	// Write message type and increment pointer
	mOut->msgType = msgType;
	mTemp += sizeof(MessageHdr);

	// Write sender address and increment pointer
    memcpy(mTemp, &memberNode->addr.addr, sizeof(Address));
	mTemp += sizeof(Address);

	// Write fail/join byte and increment pointer
    memcpy(mTemp, &db_type, sizeof(dbTypes));
	mTemp += sizeof(dbTypes);

	// Write failed/joined address
    memcpy(mTemp, &addr, sizeof(Address));

	return (char*)mOut;
}

Address MP1Node::readDeltaBuff(dbTypes* type){
	// If the delta buffer is empty, then return empty and a dummy address
	if(deltaBuff.empty()){
		*type = EMPTY;
		return memberNode->addr;
	}
	
	// Reset iterator if end was reached
	if(dbit == deltaBuff.end()){
		dbit = deltaBuff.begin();
	}
	
	Address addr(dbit->first); 
	*type = dbit->second;

	// Increment iterator
	dbit++;
	return addr;
}

void MP1Node::writeDeltaBuff(Address addr, dbTypes type){
	 
	long currTime = par->getcurrtime();
	// Find if this node is in the map
	auto it = memberMap.find(addr.getAddress());
	// Update grading log and membership map
	bool newEvent = false;
	if(type == FAILED){
		// If the node is in the map, write to grading log that the node has been removed, then remove it from map
		if(it != memberMap.end() && memberMap[addr.getAddress()].nstat != SUSPECTED){
			
			// Put this node in the queue of suspected processes
			pair<string, long> newEntry(addr.getAddress(), currTime + TREMOVE);
			suspects.push_back(newEntry);

			// Mark as suspected in memberMap to prevent duplicate addition
			memberMap[addr.getAddress()].nstat = SUSPECTED;
			newEvent = true;
		}
		
	}
	else if(type == JOINED){
		// If the node is not in the map, write to grading log that the node has joined, then add it to map
		if(it == memberMap.end()){
			log->logNodeAdd(&memberNode->addr, &addr);
			memberMap[addr.getAddress()].pstat = NOT_PINGED;
			memberMap[addr.getAddress()].nstat = NOT_SUSPECTED;
			memberMap[addr.getAddress()].seq   = 0;
			newEvent = true;
		}
	} 
	else if(type == REJUV){
			//If the process is in the suspects queue, then search through suspects and remove
			if(it != memberMap.end() && memberMap[addr.getAddress()].nstat == SUSPECTED){
				for(auto curr = suspects.begin(); curr != suspects.end(); ){
					if(curr->first.compare(addr.getAddress()) == 0){
						// Erase from the suspects map, and mark it as having responded to (some other node's) ACK
						curr = suspects.erase(curr);
						memberMap[addr.getAddress()].nstat  = NOT_SUSPECTED;
						memberMap[addr.getAddress()].pstat  = NOT_PINGED;
						memberMap[addr.getAddress()].seq++;
						// If the node still has this process in the suspects queue, then start gossiping about it
						newEvent = true;
					}
					else{
						// This handles the iterator being invalidated when erase is called
						curr++;
					}
				}
			}
	}
	else if(type == EMPTY){
		// Sender process' delta buffer was empty, do nothing
		return;
	}
	else{
		cout << "writeDeltaBuff:: Invalid Argument: type = " << (int)type << " addr =  " << addr.getAddress() << endl;
		throw std::invalid_argument("type not enumerated");		
	}

	// Update delta buffer if this is a new event to the process
	if(newEvent){
		// Need to remove any other events about this address/process from the delta buffer
		// This ensures that the node is not simply added back later after being marked as failed
		for(dbit = deltaBuff.begin(); dbit != deltaBuff.end(); dbit++){
			// If the strings are equal, then remove this element and break out of loop
			if(addr.getAddress().compare(dbit->first) == 0){
				deltaBuff.erase(dbit);
				break;
			}
		}

		// If the delta buffer is at capacity, remove an element before pushing
		if(deltaBuff.size() >= DELTA_BUFF_SIZE){
			deltaBuff.pop_back();	
		}	
		// Push new element into delta buffer
		pair<string, dbTypes> dbe(addr.getAddress(), type); 
		deltaBuff.push_front(dbe);


		// Reset the iterator
		dbit = deltaBuff.begin();
	}

}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {

	int time = par->getcurrtime();
		
	Address addr;
	size_t msgSize;

	// Get the message type, and switch on the message type
	MessageHdr* mIn = (MessageHdr*)data;
	char* mOut;
	switch(mIn->msgType){
		case JOINREQ:
		{
		
			// Add requesting node to membership list, then send full list
			addr = processJOINREQ(mIn);

			// Create JOINREP message, msgSize is modified with pass by reference
			mOut = createJOINREP(&msgSize);

			// Reply with the JOINREP message
			emulNet->ENsend(&memberNode->addr, &addr, mOut, msgSize);
			
			//clean up memory
			free(mOut);

			break;
		}	
		case JOINREP:
		{
			// Create membership list based on the vector in the JOINREP message
			processJOINREP(mIn, size);

			break;
		}
		case PING:
		{
			// Update this nodes membership list based on the ping message
			addr = processMessage((char*)mIn);

			// Create an ACK message and send it
			mOut = createMessage(ACK);
			emulNet->ENsend(&memberNode->addr, &addr, mOut, ACK_MSG_SIZE);
			free(mOut);
			
			break;
		}
		case ACK:
		{
			// Update this nodes membership list based on ACK message
			addr = processMessage((char*)mIn);

			// If this process is suspected to be failed, remove it from the suspects queue and send REJUV
			if(memberMap[addr.getAddress()].nstat == SUSPECTED){
				writeDeltaBuff(addr, REJUV);
			}

			// Mark the process as having responded and increment sequence number
			memberMap[addr.getAddress()].pstat = NOT_PINGED;
			memberMap[addr.getAddress()].seq++;
			//cout << time << " Process " << memberNode->addr.getAddress() << " got ACK from " << addr.getAddress() << endl;

			break;
		}
		case IPING:
		{	
			// Get the Sender and Process to send IPING to

			break;
		}
		case IACK:
		{
			// Send an IPING to the pro
			break;
		}
		default:
			cout << memberNode->addr.getAddress() << ": INVALID MESSAGE" << endl;
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

	Address addr;
	int currTime = par->getcurrtime();

	// Pop stale elements off the delta buffer
	dbTimer++;
	if(dbTimer >= DB_TIMEOUT){
		dbTimer = 0;
		if(!deltaBuff.empty()){
			deltaBuff.pop_back();	
			dbit = deltaBuff.begin();
		}
	} 
	// Check if there are any processes that have exceeded the cleanup time
	if(!suspects.empty() && suspects.front().second <= currTime){
		addr = Address(suspects.front().first);

		// Remove this proess from the member map and write to grading log
		memberMap.erase(suspects.front().first);
		log->logNodeRemove(&memberNode->addr, &addr);
		// Remove this element from the suspects queue
		suspects.pop_front();	
			
	}

	// Check list of processes that have been sent a PING
	// If the process has timed out, send IPING to K processes and refresh the timer 
	// TODO: This is O(n), want to only check processes that have been pinged
	/*
	map<string, long>::iterator itMap;
	for(itMap = memberMap.begin(); itMap != memberMap.end(); itMap++){
		if(itMap->second != NOT_PINGED && itMap->second != SUSPECTED && currTime - itMap->second  > TFAIL){
			// Suspect the process as failed
			writeDeltaBuff(itMap->first, FAILED);
		}	
	}	
	*/
	pingData pdata;
//	cout << currTime << "Process " << memberNode->addr.getAddress() << " processing pingData " << endl; 
	while(!pinged.empty() && pinged.front().expTime < currTime ){
		pdata = pinged.front();
		pinged.pop();
	//	cout << "currTime = " << currTime << " expTime = " << pinged.front().expTime << " for proc " << pdata.addr << "with SEQ = " << pdata.seq << endl; 
		auto mIt = memberMap.find(pdata.addr);
		/* Check if the sequence number has not been incremented, indicating that no ACK was recieved */
		if(mIt != memberMap.end() && pdata.seq == mIt->second.seq){
			cout << currTime << " Process " << memberNode->addr.getAddress() << " suspects " << pdata.addr << " as failed "  << endl;
			writeDeltaBuff(pdata.addr, FAILED);
		}
	} 			

	// Construct PING message, containing an event from the delta buffer
	char* mOut = createMessage(PING);
	// Chose M random processes to send a PING

	// Select a random element
	int s = memberMap.size();
	if( s==0){
		cout << "size is zero.. crashing " << endl;
	}


	int p = rand() % memberMap.size();
	auto it = memberMap.begin();
	std::advance(it, p);
	unsigned long attempts = 0;

	for(int i = 0; i < M && attempts < memberMap.size(); ){
		attempts++;
		addr = it->first;
		// Dont send PING to yourself
		if(addr == memberNode->addr){
			//Don't count this
			continue;
		}
//NOTE: The following commented code causes failures because if a message is dropped, the process is assumed to be failed and it will not be pinged again
/*
		// Only PING if you are not waiting for a reply
		if(memberMap[addr.getAddress()] != NOT_PINGED){
			cout << "Process " << memberNode->addr.getAddress() << " skipping process " << addr.getAddress() << " with entry " << memberMap[addr.getAddress()] << endl;
			//Don't count this
			continue;
		}
*/	
		emulNet->ENsend(&memberNode->addr, &addr, mOut, PING_MSG_SIZE);
		// Add processes to PING map
		memberMap[addr.getAddress()].pstat = PINGED;

		// Add ping event to the queue
		pdata.expTime = currTime + TFAIL;
		pdata.seq     = it->second.seq;
		pdata.addr    = it->first;
		pinged.push(pdata);
	//	cout << currTime << " Process " << memberNode->addr.getAddress() << " pinged process " << pdata.addr << "with expTime = " << pdata.expTime << "and SEQ = " << pdata.seq << endl; 
		// Go to next element, wrapping around if needed
		i++;
		it++;
		if(it == memberMap.end()){
			it = memberMap.begin();
		}
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
