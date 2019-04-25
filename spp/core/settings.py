import os

from dynaconf import LazySettings, settings


class Settings(LazySettings):
    def __init__(self):
        """
        Init function currently just initializes the object allowing
        """
        pass

    def load(self, toml_file=None):
        """
        Keyword Arguments:
        toml_file -- (default None)
        """
        config_dir = toml_file = None

        if "SPP_CONFIG" in os.environ:
            # keep thpis as legacy behavior
            config_dir = os.environ['SPP_CONFIG']
        else:
            config_dir = os.getcwd()

        if toml_file is None:
            toml_file = os.path.join(config_dir, 'settings.toml')

        dconf = {}
        dconf.setdefault('GLOBAL_ENV_FOR_DYNACONF', 'SPP')

        env_prefix = '{0}_ENV'.format(
            dconf['GLOBAL_ENV_FOR_DYNACONF']
        )  # DJANGO_ENV

        dconf.setdefault(
            'ENV_FOR_DYNACONF',
            os.environ.get(env_prefix, 'DEVELOPMENT').upper()
        )

        dconf['ROOT_PATH_FOR_DYNACONF'] = config_dir
        # Could also set SETTINGS_FILE to a list of files. If not set, dynaconf
        # will load *ALL* settings.{toml,json,py} files it finds in the root dir
        #dconf['SETTINGS_FILE_FOR_DYNACONF'] = toml_file

        super().__init__(**dconf)

        self.toml_file = toml_file
        self.config_dir = config_dir
        self.common_dir = self.COMMON


        self.nll_base = os.path.join(self.common_dir,
                                     self.get('nlloc').nll_base)



settings = Settings()
