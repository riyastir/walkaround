## Walkaround – Wave on App Engine ##

Walkaround is a variant of Wave, based on the [Apache Wave](http://incubator.apache.org/wave/) code base, that runs on App Engine.
Walkaround can [import](http://code.google.com/p/walkaround/wiki/ImportingWaves) waves from wave.google.com to allow users to keep working with their data after wave.google.com is shut down.

You can use a volunteer-maintained walkaround server at https://wavereactor.appspot.com/, or you can [run your own](http://code.google.com/p/walkaround/wiki/RunningTheCode).

Walkaround supports live collaborative rich-text editing, diff-on-open, in-line replies, user avatars, wave gadgets, attachments, full text search, and we are working on robots as well as inbox management features like archiving and muting.  For now, it does not support Wave extensions, federation, private replies, or folder management, but these features could be added.  See the [feature roadmap](FeatureRoadmap.md).

Much of the walkaround code is not specific to Wave, but factored out as a separate, more general collaboration layer that manages shared live objects.  These objects can be modified by multiple clients at the same time, with changes made by any client immediately broadcast to all others.  The Wave application is built on top of this, but the live collaboration layer is flexible enough to support other applications.  See the [design overview](DesignOverview.md).

The developer mailing list is [walkaround-dev](https://groups.google.com/group/walkaround-dev).


