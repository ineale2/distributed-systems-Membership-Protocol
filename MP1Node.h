/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5


// Message sizes
#define SMALL_MSG_SIZE (sizeof(MessageHdr) + sizeof(Address)*2 + sizeof(dbTypes))
#define PING_MSG_SIZE SMALL_MSG_SIZE
#define ACK_MSG_SIZE  SMALL_MSG_SIZE

// Tuning constants
#define M 1 //Number of processes to randomly ping
#define K 1 //Number of processes to select for indirect ping
#define NOT_PINGED -1

#define DELTA_BUFF_SIZE 10
#define NOT_ALIVE  0
#define ALIVE  1
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    PING,
    ACK,
    IPING,
    IACK,
    DUMMYLASTMSGTYPE
};

enum dbTypes{
	FAILED,
	JOINED,
	EMPTY 
};
/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */

class MP1Node {
private:
	/* Private Data */
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	map<string, long> pingMap;
	map<string, long> ipingMap; 
	map<string, short> memberMap; //Address : failed?
	deque<pair<string, dbTypes> > deltaBuff;	
	deque<pair<string, dbTypes> >::iterator dbit;
	char NULLADDR[6];

	/* Private Methods */
	Address readDeltaBuff(dbTypes*);
	void 	writeDeltaBuff(Address, dbTypes);
	char* 	createJOINREP(size_t* msgSize);
	char* 	createMessage(MsgTypes msgType);
	Address processJOINREQ(MessageHdr* mIn);
	void 	processJOINREP(MessageHdr* mIn, int size);
	Address processMessage(char* mIn);

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
