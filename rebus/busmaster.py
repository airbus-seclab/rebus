from rebus.tools.registry import Registry


class BusMasterRegistry(Registry):
    pass


class BusMaster(object):
    _name_ = "BusMaster"
    _desc_ = "N/A"

    @staticmethod
    def register(f):
        return BusMasterRegistry.register_ref(f, key="_name_")

    @classmethod
    def add_arguments(cls, subparser):
        """
        Overridden by BusMasters that have configuration parameters.

        Call add_argument on the received object to add options.
        """
        pass

    def run(self, storage, options=None):
        """
        Start running the bus.

        :param storage: initialized storage instance
        :param options: argparse.Namespace object
        """
        raise NotImplementedError
