from abc import ABC, abstractmethod

from ..core.settings import settings


class ProcessingUnit(ABC):
    def __init__(self, module_name, app=None, module_type=None):
        self.__module_name = module_name
        self.app = app
        self.module_type = module_type
        self.debug_level = settings.DEBUG_LEVEL
        self.debug_file_dir = settings.DEBUG_FILE_DIR
        self.settings = settings.sensors
        self.params = settings.get(self.module_name)

        super(ProcessingUnit, self).__init__()
        self.initializer()

    @property
    def module_name(self):
        return self.__module_name

    @abstractmethod
    def initializer(self):
        """ initialize processing unit """
