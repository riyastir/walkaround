# FAQ #




---


## How do I delete a wave? ##

You can remove all participants from the wave (click on the avatars at
the top).  With no participants, the wave won't be accessible to
anyone, and will disappear from everyone's inbox and search results.

Eventually, the server will garbage-collect waves with no
participants, but that is not implemented yet.


---


## How do I view the history of a wave? ##

There's a hacky way to do this:

  * Right-click on the wave in your inbox or search results
  * Click "open in new window" (or similar), go to that window
  * Edit the URL to add &version=10 at the end

Try out different numbers for "version" to see the history.  Versions 1, 2, and 3 will usually be empty, but with higher versions, you should start seeing content.  If the version is too high, you'll get "Internal server error".

Not very usable, but all the pieces are there, it just needs a better user interface (see [issue 113](https://code.google.com/p/walkaround/issues/detail?id=113)).