## Getting and running the code ##

Supported environments:
  * Linux with Java, [Ant](https://ant.apache.org/), [Subversion](https://subversion.apache.org/), Zip, GCC, and Git installed.  On Ubuntu, you can install these packages by running `sudo apt-get install openjdk-6-jdk ant subversion zip build-essential git-core` .
  * Mac OS X with [MacPorts](https://www.macports.org/) and a few extra packages that you can install by running `sudo port install md5sha1sum apache-ant subversion` .

Steps to run walkaround:
```
git clone https://code.google.com/p/walkaround/
cd walkaround
./get-third-party-deps
cp runant.sample runant
chmod +x runant
./runant run
```

(If `./get-third-party-deps` warns you that the certificate for `https://svn.apache.org:443` is not trusted, check that the fingerprint is `bc:5f:40:92:fd:6a:49:aa:f8:b8:35:0d:ed:27:5e:a6:64:c1:7a:1b` (from https://www.apache.org/dev/version-control.html) and enter `p` to accept permanently.)

The above steps allow you to test walkaround locally by going to `http://localhost:8080/` in your browser.  You'll see a "New wave" button after going through App Engine's fake login screen.  Running walkaround locally is only for testing; for proper persistence, user authentication and access control, and scalability, you have to deploy it to App Engine (see below).

<br>
Optionally, if you want to allow walkaround to show participant avatars and import waves from wave.google.com, you should get an OAuth application id:<br>
<ul><li>Go to <a href='https://code.google.com/apis/console/'>https://code.google.com/apis/console/</a>.  All of the settings you are about to make are easy to change later, so don't worry about them too much.<br>
</li><li>Create a new project.<br>
</li><li>Click on "API Access" on the left, then "Create an OAuth2.0 client id".<br>
</li><li>Enter a product name (like <code>MyWalkaround</code>).  You can leave the logo empty.  Click "Next".<br>
</li><li>Application type: Web application.  Next to "Your site or hostname", click on "more options".<br>
</li><li>Authorized redirect URIs: Replace the text in here with <code>http://localhost:8080/authenticate</code>.  If you have an application id on App Engine, add a second line <code>https://&lt;your-app-id&gt;.appspot.com/authenticate</code>.<br>
</li><li>Authorized JavaScript origins: Delete the text in here.<br>
</li><li>Click "Create client ID".  (I've seen the page say "An unexpected error has occurred. We're looking into it" even though the client ID was created just fine; if you get this message, reload the page a few times to check if the client ID appears.)<br>
</li><li>Copy and paste the client ID and client secret into your <code>runant</code> script.</li></ul>

<br>
To deploy walkaround to App Engine:<br>
<ul><li>Create an application ID at <a href='https://appengine.google.com'>https://appengine.google.com</a>.  Use the default High Replication storage, <b>not</b> Master/Slave.<br>
</li><li>Edit your <code>runant</code> script to put your application id in the <code>app-id</code> parameter.<br>
</li><li><code>./deploy</code>
Once you've entered your credentials, they will be stored for a day, and you can use <code>./deploy</code> to re-build and re-deploy.</li></ul>

<br>
Optionally, for better responsiveness if you have many users:<br>
<ul><li>Enable backends.  This is easy but currently not documented; ask on the <a href='https://groups.google.com/group/walkaround-dev'>mailing list</a> how to do this.</li></ul>

<br>
To update and re-deploy walkaround:<br>
<pre><code>git pull<br>
./runant war<br>
third_party/appengine-sdk/sdk/bin/appcfg.sh update_indexes build/war<br>
</code></pre>
After this, go to the Admin Console and click "Datastore Indexes".  Wait until all indexes show status "Serving".  Finally, run:<br>
<pre><code>third_party/appengine-sdk/sdk/bin/appcfg.sh update build/war<br>
</code></pre>

<br>
If <code>./runant</code> gives you compilation errors, try<br>
<pre><code>rm -rf third_party third_party_src<br>
./get-third-party-deps<br>
</code></pre>
and repeat.<br>
<br>
<br>
If you want to make changes to walkaround, see ContributingCode.