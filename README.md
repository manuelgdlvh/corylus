Decouple
Log Storage
State Machine (Will apply operation's)
Partition by term, index message to parallel sent of them to all followers

crashes after committing the log entry but before respond-
ing to the client, the client will retry the command with a
new leader, causing it to be executed a second time. The
solution is for clients to assign unique serial numbers to
every command. Then, the state machine tracks the latest
serial number processed for each client, along with the as-
sociated response. If it receives a command whose serial
number has already been executed, it responds immedi-
ately without re-executing the request.

For example, it easily supports batching and
pipelining requests for higher throughput and lower la-
tency. Various optimizations have been proposed in the
literature for other algorithms; many of these could be ap-
plied to Raft, but we leave this to future work.

State Machine component
Log component

Decouple strategy of leader discovery of network client
Add signaling
Add metrics (number of election timeouts, etc)
Reread raft paper
