# go-helix
A apache-helix implemented in golang

Originally based on https://github.com/yichen/gohelix

### Roadmap

- [ ] Full HelixAdmin
- [ ] Full HelixManager
- [ ] Full Spectator
- [ ] Controller

### Features

- Multi node
  - partitioning
  - discovery
  - co-location

- Fault tolarant
  - replication
  - fault detection
  - recovery

- Cluster expansion
  - throttle movement
  - redistribute data

### TODO

- [ ] timer tasks
- [ ] manager base
- [ ] messaging log

- [ ] chroot bug
- [ ] curator
- [ ] super cluster
- [X] tag
- [ ] too many WaitUntilConnected
- [ ] metrics
- [ ] constraint
- [ ] HelixMultiClusterController, HelixStateTransitionHandler, HelixTaskExecutor.onMessage

