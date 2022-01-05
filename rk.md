## BS self reflections
----



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

For now, default UI configuration always create new key