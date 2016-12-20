package redis

/*
class RoutingLogic {
    public void write(Request request) {
        partition = getPartition(request.key);
        List<Node> nodes = routingTableProvider.getInstance(partition, "MASTER");
        nodes.get(0).write(request);
    }

    public void read(Request request) {
        partition = getPartition(request.key);
        List<Node> nodes = routingTableProvider.getInstance(partition);
        random(nodes).read(request);
    }
}
*/
