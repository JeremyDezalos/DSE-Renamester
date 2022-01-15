## BS self reflections
----

Clearly need a better disconnect/reconnect, 
Need to fetch (GET) status from peer (diconnected or connected), then act accordingly
(POST connect or POST disconnect)
When reconnecting, also need to fetch peerInfo (new address) after success

Verify behavior
Add a disconnect, reconnect button

Add more node information (private key (and pub key eventually))
(Add private key option on connection)

Verify behavior
Then, add removal of disconnected node for non-neighbors


WARNING, TEST RELAYING WITH MORE THAN ONE HOP (I might have broken it)
ALSO, I think there is an issue in "ExecRumorMessage", because the internal rumor 
use the RumorsMessage packet header information when processing single rumors, 
I think they should use the "Origin" field inside the rumor instead 
(and create a new header with it)

signature is stored in the transport.Message, signature verification adds a 
Valid field in packets (updated during "ProcessPacket")
registry/standard/mod.go, MarshalMessage probably where we can sign?
registry/standard/mod.go, ProcesPacket probably where we can verify signature?

Some signing go well, others don't, e.g. status when embededd in ack

Next step: add renaming

Idea: change routing table (yeah... again) to have a mapping as follow

peerID -> {
    nextHopID
    IP (empty string "" for non-neighbors)
    display name
}

otherwise we will have up to 3 tables

This would allow to:
- Get a list of neighbhors somewhat efficiently
- Get a list of peerID->display name easily
- Still get the routing table easily
- (Optional) Easily create a routing table EXCLUDING neighbors (to avoid A->A mapping in UI) 


Original "AddPeer" function allows to add (mutiple) peers using only IP addresses
This as to be kept somehow for initial connection to other peers,
but internally should change to sending idRequest and idReply messages, 
which would be unicasted depending only on IP addresses
Current unicast use the routing table, this special unicast should not.

If we make headers use only IDs then the routing table translate need a special 
table for neighbhors to translate neighbors addresses to IPs.   

(letter->IDs, number->IPs)

RT A,1|RT B, 2|RT C, 3
---|---|---
A->1|B->2|C->3
B->2|A->1|A->2
C->2|C->3|B->2

Source, RealyBy, Destination (outgoing)

A to C: (A, A, C)  
C->2:B (A, B, C)  
C->3:C  

Alternatively we should/could add an "AddNeighbor", that could (for example) 
send private messages (with our IP) to a list of peers which then would send 
their IPs so that anyone can extend its list of neighbors. 



## Objectives 
----

At node creation: 
- Generate IP as done currently
- Generate ed25519 keys ID ONLY IF none is given by the configuration
    - Using base 64 to make the keys human readable

General:
- Replace destination IPs in routing table with pub key
- To properly populate this routing table, we will add two new messages: idRequest and idReply. idRequest will be sent when a peer uses the AddPeer function and will send a simple message to the given IP address. A peer that receives an idRequest will add the sender of the request to its routing table and answer with an idReply so that the requester can do the same. 
- Create id-name association table
- New message for changing name
- Display name instead of IDs(/IPs) in UI
- Sign an verify signature of chat messages

Last changes: 
- Store private/public keypair in file
- Store current message history in file


## Progress/results
----

For now, default UI configuration always create new keypairs

Routing table properly working with (base64 encoded) public keys 
Added a Neighbors table (also displayed in UI)
Added a alias (names) table (displayed in UI)
Possibility to rename (under "broadcast" tab)
Names displayed in chat instead of pub keys (when the routing table is up to date) 
Pub keys displayed on hover and copied on click in chat

!! To fix, not displayed as sender when broadcasting !!
(also message too longs or with linebreak are broken but it might have been the case from the start)

## History
----
Added idRequest/Reply  
Still updating `AddPeer`   
Using `n.conf.AckTimeout` for id request for now, maybe should have its own timeout
as a 0 value would be quite inconvenient 
(result in having to manually retry adding a peer if it fails, without any feedback)  
For now it will retry N times (with N chosen arbitrarily, N=3)  
Also AddPeer should now return and error when id didn't receive an idReply from 
some peers (not yet updated) (still need to update calls to AddPeer to take this into account)  


Not sure about updating paxos with ids

|! Warning !|
|---|
There IS an issue for now because some operation in SOME callbacks bypass the routing table and therefore will send packets with an ID instead of an IP  
Also, every node function referring to itself should now use its ID instead of IP  
List of places where a `n.conf.Socket.Send(...)` (bypassing routing) was replaced: 
- ExecRumorMessage
- ExecStatusMessage (x2)
- ExecSearchRequestMessage