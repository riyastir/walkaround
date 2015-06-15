## How to add a robot API to walkaround ##

Adding a robot API to walkaround is entirely possible, and many parts of it are straightforward, but it will take some learning and debugging to get it all to work.  This document outlines how to do it.

The robot API supports two modes, active and passive.  In the active API, the robot initiates contact with the wave server to operate on waves; in the passive API, the wave server contacts the robot to
notify it of changes to waves that it is a participant on, and the robot responds with the operations it wants to perform.

Both modes are useful.  We discuss the passive API first.


### Five simple steps ###

1. Modify `queue.xml` to add a new queue `robot-notifications` with `max-concurrent-requests` set to 1.  The `max-concurrent-requests` setting will limit the system to notify only one robot at a time; this is not scalable, but it avoids worrying about race conditions.  We'll make it scalable later (described below).

2. Add an `AbstractDirectory` that maps the pair (robot id, conv slob id) to the version number that that robot has last seen on that conv wavelet (=conv slob).  The robot id is the `ParticipantId` of the robot.  We'll call the entity kind `RobotNotified`.

3. Add a `PreCommitAction` that transactionally enqueues a task in the `robot-notifications` queue.  The robot notification will happen in this task, or other tasks that this task schedules, to keep them out of the server's critical path for writing deltas to disk.  The task can be a class `NotifyAllRobots implements Deferred` that carries the slob id (=wavelet id) and the `newVersion` (version number staged in the transaction).

We should have a short circuit check here that avoids enqueueing the task if no robots are on the wave.

To be able to use Guice from the `Deferred` task, use `GuiceSetup.getInjectorForTaskQueueTask()`.  Alternatively, define your own `Handler` to handle task queue tasks; `Handler`s can use `@Inject` to receive objects from Guice.  See `PostCommitTaskHandler` for an example.

You will very likely run into errors related to `UserContext` or `AccessChecker`; if so, ask for help on the mailing list.  You may need to wrap your handler into a call to `ServletAuthHelper.serve()` with custom implementations of `AccountLookup` and a dummy `NeedNewOAuthTokenHandler` to fit robots into Walkaround's user authorization scheme.

4. The `NotifyAllRobots` task will first determine which robots to notify.  A simple approximation is to look at the participant list of the conv wavelet, which we can obtain by feeding the output of `SlobStore.loadAtVersion()` into `WaveSerializer.deserializeWavelet()`.

Looking at the current participant list is not entirely correct -- removed robots may miss notifications for some events that happened before their removal.  But this is a corner case that we can handle later.

For each robot to notify, `NotifyAllRobots` enqueues a `NotifyRobot` task (again a subclass of `Deferred`, or a task that calls your custom `Handler`) in the `robot-notifications` queue.  `NotifyRobot` carries the robot id, slob id, and `newVersion`.

Again, it would be nice to short-circuit this if there is only one `NotifyRobot` task, and invoke it immediately rather than enqueueing it.

5. Each `NotifyRobot` task needs to read its entry in the `RobotNotified` table to find out what version the robot has seen.  If the table says that we have already notified the robot up to `newVersion`, the task returns success.  (This check may seem unnecessary but it's not -- it makes the task idempotent to avoid sending duplicate notifications if the task queue runs it multiple times.)

Otherwise, it needs to construct that version of the wavelet, use `SlobStore` to read the changes between it and `newVersion`, apply them, and generate events to send to the robot via `URLFetch`.  The logic to generate robot events based on a wavelet's state and delta history can be reused from Apache Wave.

The robot will respond with changes that it wants to make to the wavelet.  Again, we can use code from Apache Wave to translate these into deltas.  These deltas can then be passed to `SlobStore.mutateObject()`.

Once `mutateObject()` succeeds, we need to update the `RobotNotified` table to record that the robot has seen these events.


### Making it scale ###

A relatively simple way to make it scalable:

1. Introduce a `MemcacheTable` that maps pairs (robot id, conv slob id) to the `newVersion` that we are currently notifying the robot of.  The presence of an entry in this memcache indicates that a task is currently sending notifications to the robot and locks out other tasks from doing the same.

2. Modify `NotifyRobot`: First, check the `MemcacheTable` to see if another task is already notifying the robot.  If so, this task can't proceed.  If the other task is notifying the robot up to or beyond this task's `newVersion`, this task should return success (it can rely on the other task to do the work); otherwise, it should return failure so that the task queue will retry it later.

If not, put `newVersion` into the table, using `SetPolicy.ADD_ONLY_IF_NOT_PRESENT` (to avoid overwriting a concurrent put from a different task) and an expiry of 33 seconds.  If the put failed, retry later.

If the put succeeded, proceed as in the original `NotifyRobot` logic.  Change the `URLFetch` to have a 30-second timeout, and re-put our memcache entry right before (and perhaps after) every `URLFetch`.

Finally, remove the memcache entry.

3. Remove the `max-concurrent-requests` constraint from `queue.xml`.

This design is a bit of a hack, but we should try it to see what kinds of problems it has in practice.


### Adding support for the active API ###

Once the passive API is implemented, the main complication that the active API adds is the need to authenticate robots, probably via OAuth.  App Engine has an API for this, see https://code.google.com/appengine/docs/java/oauth/overview.html .

The processing code should be very similar to the passive API.