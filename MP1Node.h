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
#define DB_TIMEOUT 15


// Message sizes
#define SMALL_MSG_SIZE (sizeof(MessageHdr) + sizeof(Address)*2 + sizeof(dbTypes))
#define PING_MSG_SIZE SMALL_MSG_SIZE
#define ACK_MSG_SIZE  SMALL_MSG_SIZE

// Tuning constants
#define M 5 //Number of processes to randomly ping
#define K 1 //Number of processes to select for indirect ping

#define DELTA_BUFF_SIZE 30
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
	REJUV,
	EMPTY 
};

enum nodeStatus{
	NOT_SUSPECTED,
	SUSPECTED
};

enum pingStatus{
	NOT_PINGED,
	PINGED 
};
/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

struct nodeData{
	enum nodeStatus nstat;
	enum pingStatus pstat;
	long seq;
	nodeData(enum nodeStatus ns, enum pingStatus ps, long s): nstat(ns), pstat(ps), seq(s)
	{
	}
	nodeData() : nstat(NOT_SUSPECTED), pstat(NOT_PINGED), seq(0)
	{
	}
};

struct pingData{
	long expTime;
	long seq;
	string addr;
	pingData(long et, long s, string a) : expTime(et), seq(s), addr(a){
	}
	pingData() : expTime(0), seq(0), addr()
	{
	}
};

struct dbData{
	string addr;
	dbTypes dbType;
	long seq;
	dbData(string a, dbTypes d, long s) : addr(a), dbType(d), seq(s){
	}
	dbData() : addr(), dbType(EMPTY), seq(0){
	}
};

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
	
	/* pinged is a queue of the processes that this node has pinged */
	queue<pingData> pinged;

	/* memberMap is a map of all nodes known to this node from the address (as a string) to a struct of status for the node */
	map<string, nodeData> memberMap; 

	/* suspects is a queue of all nodes that this node suspects to be failed. 
	The pair is the address of the process and the time at which the suspected process will be removed from the member map */
	deque<pair<string, long   > > suspects;

	/* deltaBuff is a queue of events to gossip about. dbit is an iterator over the deltaBuff */
	deque<dbData> deltaBuff;	
	deque<dbData>::iterator dbit;
	int dbTimer;

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
	void initMemberListTable(Member *memberNode);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void printAddress(Address *addr);
	virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
