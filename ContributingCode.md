## Contributing code ##

The first step is to get the code to run (see RunningTheCode), and to find something that bothers you that you want to work on; see http://code.google.com/p/walkaround/issues/list for inspiration.  Then, set up Eclipse and fix it.  Finally, test your changes and send a patch to the mailing list.


### Setting up Eclipse ###

Walkaround comes with Eclipse project settings.  The following set-up instructions work on Linux, but Windows or Mac OS should be similar.

Create a new Eclipse workspace for walkaround somewhere outside of your walkaround git clone, then click File->Import...->General->Existing projects into workspace...->Next.  Make sure "Select root directory" is checked, then click Browse..., select the directory of your git clone, and click OK.  In the "Projects" box, "Walkaround" should appear, checked.  Do not select "Copy projects into workspace".  Click "Finish".

Repeat the same procedure to import the `wave-protocol` project from the subdirectory `third_party_src/wave/trunk` of your git checkout.

If Eclipse complains about missing JARs, run `get-third-party-deps` again.


### Testing ###

Before sending a patch, make sure `./runant test` passes.

To make sure the code doesn't depend on any leftover files, run `./runant clean && ./runant test`.

Please also do some manual testing of your changes and any other areas that might be affected, both in the local development environment (`./runant run`) and when deployed to App Engine (`./deploy`).


### Sending a patch ###

Feel free to send your changes as a patch to the mailing list, or to push changes to a public git repository.

Before we can accept your patch, you need to sign the appropriate contributor agreement at http://code.google.com/legal/individual-cla-v1.0.html or http://code.google.com/legal/corporate-cla-v1.0.html .  The former can be signed online in your browser.