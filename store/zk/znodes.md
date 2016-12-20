# znode layout

### cluster

    /{cluster}/CONFIGS/CLUSTER/{cluster}
    /{cluster}/PROPERTYSTORE

### controller

    /{cluster}/CONTROLLER/ERRORS
    /{cluster}/CONTROLLER/HISTORY
    /{cluster}/CONTROLLER/MESSAGES
    /{cluster}/CONTROLLER/STATUSUPDATES

### participant

    /{cluster}/CONFIGS/PARTICIPANT/{instance}
    /{cluster}/INSTANCES/{instance}/CURRENTSTATES/{sessionid}/{resource}

### resource

    /{cluster}/CONFIGS/RESOURCE
    /{cluster}/EXTERNALVIEW/{resource}
    /{cluster}/IDEALSTATES/{resource}
    /{cluster}/INSTANCES/{instance}/CURRENTSTATES/{sessionid}/{resource}
