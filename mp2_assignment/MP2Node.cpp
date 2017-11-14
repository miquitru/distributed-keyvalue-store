/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address)
{
    this->memberNode = memberNode;
    this->par = par;
    this->emulNet = emulNet;
    this->log = log;
    this->memberNode->addr = *address;
    this->isInitialized = false;
}

/**
 * Destructor
 */
MP2Node::~MP2Node()
{
    delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing()
{
    /*
     * Implement this. Parts of it are already implemented
     */
    vector<Node> curMemList;
    /*
     *  Step 1. Get the current membership list from Membership Protocol / MP1
     */
    curMemList = getMembershipList();
    
    /*
     * Step 2: Construct the ring
     */
    // Sort the list based on the hashCode
    sort(curMemList.begin(), curMemList.end());
    
    /* right now create the ring as a copy of the sorted member list */
    this->ring = curMemList;
    
    /*Check the status of replicas relative to your position in the ring */
    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
    this->RunStabilizationProtocol();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList()
{
    unsigned int i;
    vector<Node> curMemList;
    for ( i = 0 ; i < this->memberNode->memberList.size(); i++ )
    {
        Address addressOfThisMember;
        int id = this->memberNode->memberList.at(i).getid();
        short port = this->memberNode->memberList.at(i).getport();
        memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
        memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
        curMemList.emplace_back(Node(addressOfThisMember));
    }
    return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key)
{
    std::hash<string> hashFunc;
    size_t ret = hashFunc(key);
    return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value)
{
    g_transID++;
    vector<Node> recipients = findNodes(key);
    assert(recipients.size()==NUM_KEY_REPLICAS);
    
    for (int i=0;i<NUM_KEY_REPLICAS;++i)
    {
        Message createMsg = Message(g_transID,this->memberNode->addr,MessageType::CREATE,key,value,static_cast<ReplicaType>(i));
        this->UnicastMessage(createMsg,recipients[i].nodeAddress);
    }
    
    TransactionLog tr(g_transID, key, value, MessageType::CREATE,QUORUM_COUNT, par->getcurrtime());
    transactionList.push_front(tr);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key)
{
    g_transID++;
    Message readMsg(g_transID,this->memberNode->addr,MessageType::READ,key);
    
    TransactionLog tr(g_transID, key, "", MessageType::READ, QUORUM_COUNT, par->getcurrtime());
    transactionList.push_front(tr);
    
    vector<Node> recipients = findNodes(key);
    this->MulticastMessage(readMsg,recipients);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value)
{
    g_transID++;
    vector<Node> recipients = findNodes(key);
    assert(recipients.size()==NUM_KEY_REPLICAS);
    
    for (int i=0;i<NUM_KEY_REPLICAS;++i)
    {
        Message updateMsg= Message(g_transID,this->memberNode->addr,MessageType::UPDATE,key,value,static_cast<ReplicaType>(i));
        this->UnicastMessage(updateMsg,recipients[i].nodeAddress);
    }
    
    TransactionLog tr(g_transID, key, value, MessageType::UPDATE, QUORUM_COUNT, par->getcurrtime());
    transactionList.push_front(tr);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key)
{
    if (key == "invalidKey")
    {
        log->LOG(&(memberNode->addr),"invalid key recieved at client delete %s","key");
    }
    
    g_transID++;
    Message deleteMsg = Message(g_transID,this->memberNode->addr,MessageType::DELETE,key);
    TransactionLog tr(g_transID, key, "", MessageType::DELETE, QUORUM_COUNT, par->getcurrtime());
    transactionList.push_front(tr);
    
    vector<Node> recipients = findNodes(key);
    this->MulticastMessage(deleteMsg,recipients);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica)
{
    Entry newEntry(value, par->getcurrtime(), replica);
    this->keyEntryMap.insert(pair<string, Entry>(key, newEntry));
    return true;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key)
{
    map<string, Entry>::iterator it;
    if ((it = keyEntryMap.find(key)) !=keyEntryMap.end())
    {
        return it->second.convertToString();
        
    }
    else return "";
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica)
{
    Entry newEntry(value, par->getcurrtime(), replica);
    map<string, Entry>::iterator it;
    if ((it = keyEntryMap.find(key)) !=keyEntryMap.end())
    {
        keyEntryMap.insert(pair<string, Entry>(key, newEntry));
        return true;
    }
    
    return false;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key)
{
    return keyEntryMap.erase(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages()
{
    /*
     * Implement this. Parts of it are already implemented
     */
    char * data;
    int size;
    
    /*
     * Declare your local variables here
     */
    
    // dequeue all messages and handle them
    while ( !memberNode->mp2q.empty() ) {
        /*
         * Pop a message from the queue
         */
        data = (char *)memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();
        
        string message(data, data + size);
        //log->LOG(&(memberNode->addr)," message received :%s", data);
        
        this->HandleIncomingMessage(message);
    }
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key)
{
    size_t pos = hashFunction(key);
    vector<Node> addr_vec;
    if (ring.size() >= 3)
    {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode())
        {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        }
        else
        {
            // go through the ring until pos <= node
            for (int i=1; i<ring.size(); i++)
            {
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode())
                {
                    addr_vec.emplace_back(addr);
                    addr_vec.emplace_back(ring.at((i+1)%ring.size()));
                    addr_vec.emplace_back(ring.at((i+2)%ring.size()));
                    break;
                }
            }
        }
    }
    return addr_vec;
}

/* Called everytime in the recvLoop to decrement transaction timers */
void MP2Node::UpdateTransactionLogs()
{
    list<TransactionLog>::iterator it = transactionList.begin();
    while(it != transactionList.end())
    {
        if((par->getcurrtime()-it->timeStamp) > RESPONSE_WAIT_TIME)
        {
            MessageType mtype = it->transType;
            int transid = it->transID;
            this->log->LOG(&(memberNode->addr),"Transaction %d timeout",transid);
            switch(mtype)
            {
                case MessageType::CREATE: log->logCreateFail(&memberNode->addr,true,transid,it->key,it->value);break;
                case MessageType::UPDATE: log->logUpdateFail(&memberNode->addr,true,transid,it->key,it->value);break;
                case MessageType::READ: log->logReadFail(&memberNode->addr,true,transid,it->key);break;
                case MessageType::DELETE: log->logDeleteFail(&memberNode->addr,true,transid,it->key);break;
                default:break;
            }
            transactionList.erase(it++);
        }
        
        it++;
    }
    
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop()
{
    if ( memberNode->bFailed )
    {
        return false;
    }
    else
    {
        this->UpdateTransactionLogs();
        return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size)
{
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

void MP2Node::HandleIncomingMessage(Message message)
{
    switch(message.type)
    {
        //server side message types
        case MessageType::CREATE: HandleKeyCreateMsg(message);break;
        case MessageType::UPDATE: HandleKeyUpdateMsg(message);break;
        case MessageType::DELETE: HandleKeyDeleteMsg(message);break;
        case MessageType::READ: HandleKeyReadMsg(message);break;
                
        //reply messages to read requests
        case MessageType::READREPLY: HandleReadReplyMsg(message);break;
        
        //reply messages to create, update, delete requests
        case MessageType::REPLY: HandleReplyMsg(message);break;
        default:
            log->LOG(&(memberNode->addr),"Invalid packet received, not valid type msg: %s",message.toString().c_str());
    }
}


void MP2Node::HandleNodeFailure(Node toNode, ReplicaType repType)
{
     map<string, Entry>::iterator it;
     for(it= keyEntryMap.begin(); it!=keyEntryMap.end(); ++it)
     {
         if(it->second.replica == ReplicaType::PRIMARY)
         {
             Message createMsg(-1,(memberNode->addr),MessageType::CREATE,it->first, it->second.value, repType);
             this->UnicastMessage(createMsg, toNode.nodeAddress);
         }
     }
}

void MP2Node::HandleKeyCreateMsg(Message message)
 {
     Message replyMsg = Message(message.transID,(this->memberNode->addr),MessageType::REPLY,false);
     
     if(createKeyValue(message.key,message.value,message.replica))
     {
         replyMsg.success=true;
         log->LOG(&(memberNode->addr),"create server node with replica %d for key %s",message.replica,message.key.c_str());
         log->logCreateSuccess(&memberNode->addr,false,message.transID,message.key,message.value);
     }
     else
     {
         replyMsg.success=false;
         log->logCreateFail(&memberNode->addr,false,message.transID,message.key,message.value);
     }
     
     this->UnicastMessage(replyMsg, message.fromAddr);
 }
 
void MP2Node::HandleKeyUpdateMsg(Message message)
{
    Message replyMsg = Message(message.transID,(this->memberNode->addr),MessageType::REPLY,false);
    
    if(updateKeyValue(message.key,message.value,message.replica))
    {
        replyMsg.success=true;
        log->LOG(&(memberNode->addr),"update server node with replica %d for key %s",message.replica,message.key.c_str());
        log->logUpdateSuccess(&memberNode->addr,false,message.transID,message.key,message.value);
    }
    else
    {
        replyMsg.success=false;
        log->logUpdateFail(&memberNode->addr,false,message.transID,message.key,message.value);
    }
    
    this->UnicastMessage(replyMsg, message.fromAddr);
 }

void MP2Node::HandleKeyDeleteMsg(Message message)
{
    Message replyMsg = Message(message.transID,(this->memberNode->addr),MessageType::REPLY,false);
    
    if( deletekey(message.key))
    {
        replyMsg.success=true;
        log->LOG(&(memberNode->addr),"delete server node with replica %d for key %s",message.replica,message.key.c_str());
        log->logDeleteSuccess(&memberNode->addr,false,message.transID,message.key);
    }
    else
    {
        replyMsg.success=false;
        log->logDeleteFail(&memberNode->addr,false,message.transID,message.key);
    }
 
    this->UnicastMessage(replyMsg, message.fromAddr);
 }

void MP2Node::HandleKeyReadMsg(Message message)
{
    string keyval = readKey(message.key);
    if(!keyval.empty())
    {
        log->LOG(&(memberNode->addr),"read server node with replica %d for key %s",message.replica,message.key.c_str());
        log->logReadSuccess(&memberNode->addr,false,message.transID,message.key,keyval);
    }
    else
    {
        log->logReadFail(&memberNode->addr,false,message.transID,message.key);
    }
    
    Message replyMsg = Message(message.transID,(this->memberNode->addr),keyval);
    this->UnicastMessage(replyMsg, message.fromAddr);
}
 
void MP2Node::HandleReadReplyMsg(Message message)
{
    // expects to received min quorum message and will log read transaction as successfull and will consider value with
    // lastest timestamp.
    
    string msgValue= message.value;
    if(msgValue.empty())
    {
        return;
    }
    
    //split the value to extract tokens "value :timestamp: replicatype"
    string delim = ":";
    vector<string> tuples;
    int start = 0;
    int pos = 0;
    while((pos = msgValue.find(delim,start))!= string::npos)
    {
        string token = msgValue.substr(start,pos-start);
        tuples.push_back(token);
        start = pos+1;
    }
    
    tuples.push_back(msgValue.substr(start));
    assert(tuples.size()==3);
    
    string keyValue = tuples[0];
    int timeStamp = stoi(tuples[1]);
    int transid = message.transID;
    
    list<TransactionLog>::iterator it;
    for (it = transactionList.begin(); it!=transactionList.end();++it)
    {
        if(it->transID == transid)
        {
            break;
        }
        
    }
    
    if (it == transactionList.end())
    {
        this->log->LOG(&(memberNode->addr),"Transaction id not found handling readreply message", transid);
        return;
    }
    
    Address* myAddress = &(memberNode->addr);
    
    if(--(it->minQuorum)==0)
    {
        //Expected quorum replies received
        log->LOG(myAddress,"Received reply for transaction %d and id: %d,%d replies remaining",it->transType,it->transID,it->minQuorum);
        log->logReadSuccess(&memberNode->addr,true,message.transID,it->key,it->value);
        transactionList.erase(it);
    }
    else
    {
        log->LOG(myAddress,"Received reply for transaction %d and id: %d,%d replies remaining",it->transType,it->transID,it->minQuorum);
        if(timeStamp >= it->lastTimeStamp)
        {
            it->value = keyValue;
            it->lastTimeStamp = timeStamp;
            
            log->LOG(myAddress, "Changing latest val for transid :%d and key: %s",it->transID,it->key.c_str());
        }
    }
}

void MP2Node::HandleReplyMsg(Message message)
{
    int transid = message.transID;
    list<TransactionLog>::iterator it;
    
    for (it = transactionList.begin(); it!=transactionList.end();++it)
    {
        if(it->transID == transid)
        {
            break;
        }
    }
    
    if (it == transactionList.end() || !message.success)
    {
        return;
    }
    

    Address* myAddress = &(memberNode->addr);
    
    if(--(it->minQuorum)==0)
    {
        //Expected quorum replies received
        log->LOG(myAddress,"Received reply for transaction %d and id: %d,%d replies remaining",it->transType,it->transID,it->minQuorum);
        switch(it->transType)
        {
            case MessageType::CREATE: log->logCreateSuccess(&memberNode->addr,true,message.transID,it->key,it->value);break;
            case MessageType::UPDATE: log->logUpdateSuccess(&memberNode->addr,true,message.transID,it->key,it->value);break;
            case MessageType::DELETE: log->logDeleteSuccess(&memberNode->addr,true,message.transID,it->key);break;
            default:break;
        }
        transactionList.erase(it);
    }
    else
    {
        log->LOG(myAddress,"Received reply for transaction %d and id: %d,%d replies remaining",it->transType,it->transID,it->minQuorum);
    }
}

void MP2Node::MulticastMessage(Message message,vector<Node>& recipients)
{
    Address* sendaddr = &(this->memberNode->addr);
    string strrep = message.toString();
    char * msgstr = (char*)strrep.c_str();
    size_t msglen = strlen(msgstr);
    log->LOG(&(memberNode->addr), "Sending the client request : %s of length %d", msgstr, msglen);
    
    for (size_t i=0; i < recipients.size();++i)
    {
        this->emulNet->ENsend(sendaddr,&(recipients[i].nodeAddress),msgstr, msglen);
    }
}


void MP2Node::UnicastMessage(Message msg, Address& toAddr)
{
    Address* sendaddr = &(this->memberNode->addr);
    string strrep = msg.toString();
    char * msgstr = (char*)strrep.c_str();
    size_t msglen = strlen(msgstr);
    log->LOG(sendaddr, "Sending the client request : %s  of length %d", msgstr, msglen);
    this->emulNet->ENsend(sendaddr,&toAddr,msgstr, msglen);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::RunStabilizationProtocol()
{
    int nodeIndex =0;
    for (nodeIndex = 0; nodeIndex < ring.size(); ++nodeIndex)
    {
        if((ring[nodeIndex].nodeAddress==this->memberNode->addr))
        {
            break;
        }
    }
    
    int ringSize = (int)this->ring.size();
    
    // Initialize the list of neighbours
    Node prevNode2 = ring[((nodeIndex - 2) < 0? nodeIndex-2 + ringSize : nodeIndex-2) % ringSize];
    Node prevNode1 = ring[((nodeIndex - 1) < 0? nodeIndex-1 + ringSize : nodeIndex-1) % ringSize];
    Node nextNode1 = ring[(nodeIndex + 1)% ringSize];
    Node nextNode2 = ring[(nodeIndex + 2)% ringSize];
    
    Address* myAddress = &(memberNode->addr);
    
    if(!this->isInitialized)
    {
        log->LOG(myAddress,
                 "Created the member tables initially at ring pos %s",
                 ring[nodeIndex].nodeAddress.getAddress().c_str());
        this->isInitialized = true;
    }
    else
    {
        log->LOG(myAddress,
                 "Created the neighbours, position values are %s,%s,<%s>,%s,%s",
                 prevNode2.nodeAddress.getAddress().c_str(),
                 prevNode1.nodeAddress.getAddress().c_str(),
                 ring[nodeIndex].nodeAddress.getAddress().c_str(),
                 nextNode1.nodeAddress.getAddress().c_str(),
                 nextNode2.nodeAddress.getAddress().c_str());
        
        if(!(nextNode1.nodeAddress == hasMyReplicas[0].nodeAddress))
        {
            HandleNodeFailure(nextNode1, ReplicaType::SECONDARY);
        }
        else if(!(nextNode2.nodeAddress==hasMyReplicas[1].nodeAddress))
        {
            HandleNodeFailure(nextNode2,ReplicaType::TERTIARY);
        }
    }
    
    //update the has-have tables
    haveReplicasOf.clear();
    hasMyReplicas.clear();
    haveReplicasOf.push_back(prevNode2);
    haveReplicasOf.push_back(prevNode1);
    hasMyReplicas.push_back(nextNode1);
    hasMyReplicas.push_back(nextNode2);
}
