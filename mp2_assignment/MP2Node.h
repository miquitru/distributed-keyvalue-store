/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"
#include <list>

#define NUM_KEY_REPLICAS 3
#define QUORUM_COUNT 2
#define RESPONSE_WAIT_TIME 20
/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */

// struct to save requested transactions to the server
struct TransactionLog
{
public:
    int transID;
    string key;
    string value;
    MessageType transType;
    int minQuorum;
    int timeStamp;
    int lastTimeStamp;
    
    TransactionLog(int Id, string k, string val, MessageType ttype,int minQ, int tStamp):
    transID(Id),
    key(k),
    value(val),
    transType(ttype),
    minQuorum(minQ),
    timeStamp(tStamp),
    lastTimeStamp(0)
    {
        
    }
};

class MP2Node
{
private:
    // Vector holding the next two neighbors in the ring who have my replicas
    vector<Node> hasMyReplicas;
    // Vector holding the previous two neighbors in the ring whose replicas I have
    vector<Node> haveReplicasOf;
    // Ring
    vector<Node> ring;
    // Member representing this member
    Member *memberNode;
    // Params object
    Params *par;
    // Object of EmulNet
    EmulNet * emulNet;
    // Object of Log
    Log * log;
    
    map<string, Entry> keyEntryMap;
    list<TransactionLog> transactionList;
    
    bool isInitialized;
    
    // Server side Handlers
    void HandleKeyCreateMsg(Message message);
    void HandleKeyUpdateMsg(Message message);
    void HandleKeyDeleteMsg(Message message);
    void HandleKeyReadMsg(Message message);
    void HandleReadReplyMsg(Message message);
    void HandleReplyMsg(Message message);
    
    void UpdateTransactionLogs();
    
    void RunStabilizationProtocol();
    void UnicastMessage(Message msg, Address& toAddr);
    void HandleIncomingMessage(Message msg);
    void MulticastMessage(Message msg, vector<Node>& recp);
    void HandleNodeFailure(Node toNode, ReplicaType rType);
    
    
public:
    
    MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
    Member * getMemberNode()
    {
        return this->memberNode;
    }
    
    // ring functionalities
    void updateRing();
    vector<Node> getMembershipList();
    size_t hashFunction(string key);
    void findNeighbors();
    
    // client side CRUD APIs
    void clientCreate(string key, string value);
    void clientRead(string key);
    void clientUpdate(string key, string value);
    void clientDelete(string key);
    
    // receive messages from Emulnet
    bool recvLoop();
    static int enqueueWrapper(void *env, char *buff, int size);
    
    // handle messages from receiving queue
    void checkMessages();
    
    // find the addresses of nodes that are responsible for a key
    vector<Node> findNodes(string key);
    
    // server
    bool createKeyValue(string key, string value, ReplicaType replica);
    string readKey(string key);
    bool updateKeyValue(string key, string value, ReplicaType replica);
    bool deletekey(string key);
    
    ~MP2Node();
};

#endif /* MP2NODE_H_ */
