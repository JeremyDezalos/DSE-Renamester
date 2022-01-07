## BS self reflections
----

Original "AddPeer" function allows to add (mutiple) peers using only IP addresses
This as to be kept somehow for initial connection to other peers,
but internally should change to sending idRequest and idReply messages, 
which would be unicasted depending only on IP addresses
Current unicast use the routing table, this special unicast should not.

If we make headers use only IDs then the routing table translate this ID to an IP 
to send to, this IP can either be direct or indirect, the previous remark on 
unicast still holds.   
"Issue": we cannot recognize neighbors anymore because we can't check 
"origin == relay" in the routing table map, BUT the relay list always contains 
the neighbor set (AKA not a real issue mb soz, see getNeighbors).

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

Added idRequest/Reply  
Still updating `AddPeer`   
Using `n.conf.AckTimeout` for id request for now, maybe should have its own timeout
as a 0 value would be quite inconvenient 
(result in having to manually retry adding a peer if it fails, without any feedback)  
For now it will retry N times (with N chosen arbitrarily, N=3)  
Also AddPeer should now return and error when id didn't receive an idReply from 
some peers (not yet updated)  
### Warning !!
---
There IS and issue for now because some operation in SOME callbacks bypass 
the routing table and therefore will send packets with an ID instead of an IP  
Also, every node function referring to itself should now use its ID instead of IP  |
