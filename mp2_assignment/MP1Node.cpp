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
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address)
{
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
int MP1Node::recvLoop()
{
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
void MP1Node::nodeStart(char *servaddrstr, short servport)
{
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
int MP1Node::initThisNode(Address *joinaddr)
{
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
int MP1Node::introduceSelfToGroup(Address *joinaddr)
{
    MessageHdr *msg;
    
    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr)))
    {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        
        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        
        #ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Message JOINREQ sent to %s", joinaddr->getAddress().c_str());
        #endif
        
        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, (int)msgsize);
        
        free(msg);
    }
    
    return 1;
    
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode()
{
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop()
{
    if (memberNode->bFailed)
    {
        return;
    }
    
    // Check my messages
    checkMessages();
    
    // Wait until you're in the group...
    if( !memberNode->inGroup )
    {
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
bool MP1Node::recvCallBack(void *env, char *data, int size )
{
    MessageHdr * msg = (MessageHdr *)data;
    
    switch (msg->msgType)
    {
        case JOINREQ:
            return JOINREQHandler(data + sizeof(MessageHdr), size - sizeof(MessageHdr));
        case JOINREP:
            return JOINREPHandler(data + sizeof(MessageHdr), size - sizeof(MessageHdr));
        case GOSSIP:
            return GOSSIPHandler(data + sizeof(MessageHdr), size - sizeof(MessageHdr));
        case GOSSIPREPLY:
            return GOSSIPREPLYHandler(data + sizeof(MessageHdr), size - sizeof(MessageHdr));
        case DUMMYLASTMSGTYPE:
            return false;
    }
    
    return false;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps()
{
    
    if (memberNode->memberList.size() > 1)
    {
        memberNode->memberList.begin()->heartbeat++;
        memberNode->memberList.begin()->timestamp = par->getcurrtime();
        
        int pos = rand() % (memberNode->memberList.size() - 1) + 1;
        MemberListEntry& member = memberNode->memberList[pos];
        
        if (par->getcurrtime() - member.timestamp > TFAIL)
        {
            return;
        }
        
        Address memberAddress = AddressFrom(member.id, member.port);
        SendMemberShipTableTo(GOSSIP, &memberAddress);
    }
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
    
    memset(&joinaddr, 0, sizeof(memberNode->addr.addr));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;
    
    return joinaddr;
}

void MP1Node::UpdateMemberListEntry(MemberListEntry& entry)
{
    UpdateMemberListEntry(entry.getid(), entry.getport(), entry.getheartbeat());
}

void MP1Node::UpdateMemberListEntry(int id, short port, long heartbeat)
{
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it)
    {
        if (it->id == id && it->port ==port)
        {
            if (heartbeat > it->heartbeat)
            {
                it->setheartbeat(heartbeat);
                it->settimestamp(par->getcurrtime());
            }
            return;
        }
    }
    
    MemberListEntry newEntry(id, port, heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(newEntry);
    
#ifdef DEBUGLOG
    Address newAddress = AddressFrom(id, port);
    log->logNodeAdd(&memberNode->addr, &newAddress);
#endif
    
}

Address MP1Node::AddressFrom(int id, short port)
{
    Address address;
    memcpy(&address.addr[0], &id, sizeof(int));
    memcpy(&address.addr[4], &port, sizeof(short));
    return address;
}

void MP1Node::SendMemberShipTableTo(MsgTypes msgType, Address* peerAddres)
{
    long memberShipTableSize = memberNode->memberList.size();
    size_t msgSize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long) + memberShipTableSize * (sizeof(int) + sizeof(short) + sizeof(log));
    
    MessageHdr * msg = (MessageHdr *) malloc(msgSize * sizeof(char));
    char * msgPackage = (char*)(msg + 1);
    
    msg->msgType = msgType;
    
    // who is sending
    memcpy(msgPackage, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    msgPackage += sizeof(memberNode->addr.addr);
    char* numberOfValidItemsPointer = msgPackage; // reserved for the number of valid items in the table
    msgPackage += sizeof(long);
    
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end();)
    {
        if (it != memberNode->memberList.begin())
        {
            if (par->getcurrtime() - it->timestamp > TREMOVE)
            {
                
                #ifdef DEBUGLOG
                Address itemAddress = AddressFrom(it->id, it->port);
                log->logNodeRemove(&memberNode->addr, &itemAddress);
                #endif
                
                memberShipTableSize--;
                it = memberNode->memberList.erase(it);
                continue;
            }
            
            if (par->getcurrtime() - it->timestamp > TFAIL)
            {
                memberShipTableSize--;
                ++it;
                continue;
            }
        }
        
        memcpy(msgPackage, &(it->id), sizeof(int));
        msgPackage += sizeof(int);
        memcpy(msgPackage , &(it->port), sizeof(short));
        msgPackage += sizeof(short);
        memcpy(msgPackage , &(it->heartbeat), sizeof(long));
        msgPackage += sizeof(long);

        //msgPackage += AppendMemberListEntryIntoMsgPackage(*it, msgPackage);
        ++it;
    }
   
    
    memcpy(numberOfValidItemsPointer, &memberShipTableSize, sizeof(long));
    size_t msgNewSize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long) + memberShipTableSize * (sizeof(int) + sizeof(short) + sizeof(log));
    
    emulNet->ENsend(&memberNode->addr, peerAddres, (char *)msg, msgNewSize);
    free(msg);
}

bool MP1Node::JOINREQHandler(char* msgPackage, int size)
{
    Address requester;
    long heartbeat;
    
    memcpy(requester.addr, msgPackage, sizeof(memberNode->addr.addr));
    memcpy(&heartbeat, msgPackage + sizeof(memberNode->addr.addr), sizeof(long));
    
    int id = *(int*)(&requester.addr);
    int port = *(short*)(&requester.addr[4]);
    
    UpdateMemberListEntry(id, port, heartbeat);
    SendMemberShipTableTo(JOINREP, &requester);
    return true;
}

bool MP1Node::UpdateMemberListFromReceivedTable(char *msgPackage, int size)
{
    if (size < (int)(sizeof(long)))
    {
        // package size not expected
        return false;
    }
    
    long numEntries;
    memcpy(&numEntries, msgPackage, sizeof(long));
    msgPackage += sizeof(long);
    size -= sizeof(long);
    
    if (size < (int)(numEntries * (sizeof(int) + sizeof(short) + sizeof(log))))
    {
        // not enough info for all table entries received
        return false;
    }
    
    MemberListEntry currentEntry;
    for (long i = 0; i < numEntries; i++)
    {
        memcpy(&currentEntry.id, msgPackage, sizeof(int));
        msgPackage += sizeof(int);
        memcpy(&currentEntry.port, msgPackage, sizeof(short));
        msgPackage += sizeof(short);
        memcpy(&currentEntry.heartbeat, msgPackage, sizeof(long));
        msgPackage += sizeof(long);
        currentEntry.timestamp = par->getcurrtime();
        
        UpdateMemberListEntry(currentEntry);
    }
    
    return true;
}

bool MP1Node::JOINREPHandler(char *msgPackage, int size)
{
    Address senderAddress;
    memcpy(senderAddress.addr, msgPackage, sizeof(memberNode->addr.addr));
    msgPackage += sizeof(memberNode->addr.addr);
    size -= sizeof(memberNode->addr.addr);
    
    if (!UpdateMemberListFromReceivedTable(msgPackage, size))
    {
        return false;
    }
    
    memberNode->inGroup = true;
    return true;
}

bool MP1Node::GOSSIPHandler(char *msgPackage, int size)
{
    if (size < (int)(sizeof(memberNode->addr.addr)))
    {
        return false;
    }
    
    Address senderAddress;
    memcpy(senderAddress.addr, msgPackage, sizeof(memberNode->addr.addr));
    msgPackage += sizeof(memberNode->addr.addr);
    size -= sizeof(memberNode->addr.addr);
    
    if (!UpdateMemberListFromReceivedTable(msgPackage, size))
    {
        return false;
    }
    
    // sends a GossipReply message
    size_t msgSize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr);
    MessageHdr * gossipReplyMsg = (MessageHdr *) malloc(msgSize * sizeof(char));
    gossipReplyMsg->msgType = GOSSIPREPLY;
    memcpy((char *)(gossipReplyMsg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    
    emulNet->ENsend(&memberNode->addr, &senderAddress, (char *)gossipReplyMsg, msgSize);
    free(gossipReplyMsg);
    
    return true;
}

bool MP1Node::GOSSIPREPLYHandler(char* msgPackage, int size)
{
    if (size < (int)(sizeof(memberNode->addr.addr)))
    {
        return false;
    }
    
    Address senderAddress;
    memcpy(senderAddress.addr, msgPackage, sizeof(memberNode->addr.addr));
    
    int senderId = *(int*)(&senderAddress.addr);
    int senderPort = *(short*)(&senderAddress.addr[4]);
    vector<MemberListEntry>::iterator it = memberNode->memberList.begin() + 1;
    for (; it != memberNode->memberList.end(); ++it)
    {
        if (it->id == senderId && it->port == senderPort)
        {
            it->heartbeat++;
            it->timestamp = par->getcurrtime();
            return true;
        }
    }
    
    return false;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
    
    int id = *(int*)(&memberNode->addr.addr);
    int port = *(short*)(&memberNode->addr.addr[4]);
    MemberListEntry memberEntry(id, port, 0, par->getcurrtime());
    memberNode->memberList.push_back(memberEntry);
    memberNode->myPos = memberNode->memberList.begin();
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
