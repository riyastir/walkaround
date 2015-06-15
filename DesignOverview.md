## Design Overview ##

### Live Collaboration ###

The walkaround code base is split into `wave` and `slob` packages.  The `slob` package implements a layer that manages _shared live objects_ (_slobs_) – state that can be shared between multiple clients with live synchronization.  The slob layer includes client connection and reconnection, broadcasting, buffering of client-side changes and resynchronization after a temporary loss of connection, load balancing, and persistence.

The meaning and behavior of these shared live objects – how an object's state and changes to the state are defined, how clients display and operate on them – is entirely up to the application.  The application also defines the conflict resolution mechanism for concurrent modifications.  The slob layer has optional hooks to allow the application to perform access control and synchronous or asynchronous post-processing on the server after receiving changes from clients.

The `wave` package uses [Apache Wave](http://incubator.apache.org/wave/) code to build a communication and collaboration application similar to Google Wave on top of the slob layer.  Each conversation (wavelet) is a slob shared between the participants; in addition, the per-conversation per-user read/unread state is another slob that is private to that user and shared only between different browsers showing the same wave to the same user.

Even though the split between the wave and slob layers is roughly in the right place, the slob APIs are still evolving and we are making incompatible changes.  Use whatever parts of it are useful for your applications but be aware that the interfaces are not stable.

Walkaround's slob layer has similarity with [ShareJS](https://github.com/josephg/ShareJS) but is built in Java on App Engine rather than coffeescript on node.js, and designed to benefit from App Engine's automatic scaling.


### Defining Object Behavior ###


A slob type needs to define what state its instances have, and how changes to its instances work.  This is similar to a class that defines what fields each instance has and what methods are available and what they do – with the slight difference that the slob layer only cares about methods that change the state, since methods without side-effects don't need to be taken into account for remote synchronization of object state.  In addition, the slob type needs to define what happens when multiple parties concurrently make changes to an object.  The code currently assumes that operational transformation (OT) is used for this, so the slob type needs to define a transform function.  Finally, the slob type needs to define the wire format of object states and changes.

Application code defines slob types by implementing SlobModel.  Applications can define multiple slob types and use them at the same time.

Although not required, it is easiest to run the same slob code on the server and on the client.  The Wave application does this by using a Java implementation of SlobModel that runs on the server and compiles it with GWT to run on the client.  Alternatively, one could define slob types in JavaScript and write a SlobModel implementation that runs it on the server in Rhino on App Engine.


### Scalability ###


Walkaround's slob layer was designed to scale to very many slobs (=waves, for the Wave application), as long as not too many clients operate on the same slob at the same time.  The code can support tens of live clients on the same slob at the same time, and many more that get a static read-only view and poll for updates from time to time rather than receiving live updates pushed from the server; for live clients, we could probably go higher by an order of magnitude or more, but we'd have to see where the bottlenecks are and how to fix them.  The size of each slob should not exceed a megabyte or so; again, we could go higher.  We haven't thought about how many different slobs a single client can connect to at the same time.

There are a few known inefficiencies, mainly around lack of caching and unnecessary serialization/deserialization, but we know how to fix them if they become problems.

To make live concurrent editing responsive and avoid datastore contention, the slob layer dynamically assigns each slob to an App Engine backend instance and routes all changes from clients to the corresponding instance.  If multiple clients are changing the same slob at the same time, the backend instance will resolve conflicts and batch the changes into one datastore transaction to improve throughput.

As another efficiency improvement, the client sends changes to the server in batches and has only one request per slob in flight at a time.


### Persistence ###


Slobs are stored as an append-only list of changes with occasional snapshots; this is simple and fast and allows cheap reconstruction of any historical state.  Each slob is its own entity group.

Walkaround writes changes to the datastore before broadcasting them to other clients; this avoids the need for complicated rollback protocols.