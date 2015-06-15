## Notes on development and debugging ##


### Speeding up client compilation ###

Edit `src/com/google/walkaround/wave/client/Walkaround.gwt.xml` and remove some of the values from the `user.agent` property.  Leave only the type of browser you're testing with.  Chrome is the same as Safari here.


### Disabling JavaScript obfuscation for nicer client stack traces ###

Edit `build.xml`, find the word `OBFUSCATED`, and replace it with `PRETTY` or `DETAILED`.


### Cleaning and rebuilding everything from scratch ###

Do this if you get a compilation error that you can't explain, or if you're not sure that the code that's running is really the code that you see in your editor.
```
rm -rf third_party third_party_src
./get-third-party-deps
```
```
ant -f build-proto.xml clean compile
./runant clean war
```
The first part deletes and re-downloads the third-party dependencies; skip it only if you are sure your problem has nothing to do with them.


### Rebuilding `proto_src` after protobuf changes ###

After editing any of walkaround's `.proto` files, run
```
ant -f build-proto.xml
./runant clean war
```
to rebuild the files derived from them.