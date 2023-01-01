# Unicast Cache
The goal of this program is to serve as a middleware between a [unicast](https://github.com/pedromsilvapt/unicast/) media server instance and a receiver.

The goal is to be able to have an optional field in the receiver plugins, like for the Kodi config, for example, where the address of the unicache server can be entered.

In the future, it may be possible to configure the unicache address directly in the unicast server itself.

When that address is present, media requests to the unicast server are routed through the cache server instead. This cache server can then serve the media on demand, seamlessly switching between cached and uncached data.

## Storage Modes
The unicache server can be configured in two storage modes:
 - Temporary
 - Permanent

When it is configured as **temporary**, any cached media will be copied to a certain location in the disk, but it will not necessarily follow the same structure as in the original unicast server's repository. Using this mode, several lifetime strategies can then be configured.

When it is configured as **permanent**, a mapping strategy will ensure that there is a 1-1 mapping for every media, representing where it is stored in the original server, and where it should be stored in the cache server. Media stored in this manner will not be automatically purged, instead, they cached, they will be retained indefinitely.

Both modes could be used in the same cache node at the same time, and dynamic rules can be set up to determine what media uses which mode.

## Transfer Modes
*TBD*
