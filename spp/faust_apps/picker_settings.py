import os

from dynaconf import LazySettings, settings

from loguru import logger

class Settings(LazySettings):
    def __init__(self):
        """
        Init function currently just initializes the object allowing
        """
        pass

    def load(self, toml_file):
        """
        Keyword Arguments:
        toml_file -- (default None)
        """
        # config_dir = toml_file = None

        # if "SPP_CONFIG" in os.environ:
        #     # keep thpis as legacy behavior
        #     config_dir = os.environ['SPP_CONFIG']
        # else:
        #     config_dir = os.getcwd()

        dconf = {}
        dconf.setdefault('ENVVAR_PREFIX_FOR_DYNACONF', 'SPP_PICKER')

        env_prefix = '{0}_ENV'.format(
            dconf['ENVVAR_PREFIX_FOR_DYNACONF']
        )  # SPP_ENV

        dconf.setdefault(
            'ENV_FOR_DYNACONF',
            os.environ.get(env_prefix, 'DEVELOPMENT').upper()
        )

        # print(config_dir)
        dconf['ROOT_PATH_FOR_DYNACONF'] = '.'
        # Could also set SETTINGS_FILE to a list of files. If not set, dynaconf
        # will load *ALL* settings.{toml,json,py} files it finds in the root dir
        dconf['SETTINGS_FILE_FOR_DYNACONF'] = toml_file

        print(dconf)

        super().__init__(**dconf)

        self.toml_file = toml_file
        # self.config_dir = config_dir
        if hasattr(self, "COMMON"):
            self.common_dir = self.COMMON
        elif hasattr(self, "SPP_COMMON"):
            self.common_dir = self.SPP_COMMON


settings = Settings()
settings.load('picker.toml')
