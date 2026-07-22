from pathlib import Path
import yaml


def _load_version():
    # Find the location of the current file.
    #
    self_path = Path(__file__)
    self_dir = self_path.resolve().parent

    # Probe for 'cor_recipe_utils_version.yml'; if not found, fall back
    # on 'cor.yml'.
    #
    yml_file = self_dir / 'cor_recipe_utils_version.yml'
    yml_path = ('cor', 'recipe_utils', 'version')
    if not yml_file.exists():
        yml_file = self_dir / 'cor.yml'
        yml_path = ('cor', 'cli', 'version')

    # Read version from the selected yml file.
    #
    with open(yml_file, 'r') as f:
        y = yaml.safe_load(f)
        for p in yml_path:
            if y is None:
                break
            y = y.get(p)

    return y


COR_RECIPE_UTILS_VERSION = _load_version()
