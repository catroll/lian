"""
http://api.mongodb.com/python/current/api/pymongo/monitoring.html
"""

from pymongo import monitoring


class CommandLogger(monitoring.CommandListener):
    def __init__(self, logger):
        self.logger = logger

    def started(self, event):
        self.logger.info("Command {0.command_name} with request id "
                         "{0.request_id} started on server "
                         "{0.connection_id}".format(event))

    def succeeded(self, event):
        self.logger.info("Command {0.command_name} with request id "
                         "{0.request_id} on server {0.connection_id} "
                         "succeeded in {0.duration_micros} "
                         "microseconds".format(event))

    def failed(self, event):
        self.logger.info("Command {0.command_name} with request id "
                         "{0.request_id} on server {0.connection_id} "
                         "failed in {0.duration_micros} "
                         "microseconds".format(event))


class ServerLogger(monitoring.ServerListener):
    def __init__(self, logger):
        self.logger = logger

    def opened(self, event):
        self.logger.info("Server {0.server_address} added to topology "
                         "{0.topology_id}".format(event))

    def description_changed(self, event):
        previous_server_type = event.previous_description.server_type
        new_server_type = event.new_description.server_type
        if new_server_type != previous_server_type:
            self.logger.info(
                "Server {0.server_address} changed type from "
                "{0.previous_description.server_type_name} to "
                "{0.new_description.server_type_name}".format(event))

    def closed(self, event):
        self.logger.warning("Server {0.server_address} removed from topology "
                            "{0.topology_id}".format(event))


class HeartbeatLogger(monitoring.ServerHeartbeatListener):
    def __init__(self, logger):
        self.logger = logger

    def started(self, event):
        self.logger.info("Heartbeat sent to server "
                         "{0.connection_id}".format(event))

    def succeeded(self, event):
        self.logger.info("Heartbeat to server {0.connection_id} "
                         "succeeded with reply "
                         "{0.reply.document}".format(event))

    def failed(self, event):
        self.logger.warning("Heartbeat to server {0.connection_id} "
                            "failed with error {0.reply}".format(event))


class TopologyLogger(monitoring.TopologyListener):
    def __init__(self, logger):
        self.logger = logger

    def opened(self, event):
        self.logger.info("Topology with id {0.topology_id} "
                         "opened".format(event))

    def description_changed(self, event):
        self.logger.info("Topology description updated for "
                         "topology id {0.topology_id}".format(event))
        previous_topology_type = event.previous_description.topology_type
        new_topology_type = event.new_description.topology_type
        if new_topology_type != previous_topology_type:
            self.logger.info(
                "Topology {0.topology_id} changed type from "
                "{0.previous_description.topology_type_name} to "
                "{0.new_description.topology_type_name}".format(event))

    def closed(self, event):
        self.logger.info("Topology with id {0.topology_id} "
                         "closed".format(event))


def register(logger):
    monitoring.register(CommandLogger(logger))
    monitoring.register(ServerLogger(logger))
    monitoring.register(HeartbeatLogger(logger))
    monitoring.register(TopologyLogger(logger))


def __test():
    import logging
    import sys

    from motor import MotorClient

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s %(module)s:%(lineno)d %(message)s')
    register(logging)

    client = MotorClient()
    client.test.collection.insert({'message': 'hi!'})


if __name__ == '__main__':
    __test()
