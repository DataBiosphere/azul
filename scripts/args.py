import argparse
import shutil


class AzulArgumentHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):

    def __init__(self, prog: str):
        super().__init__(prog,
                         max_help_position=50,
                         width=min(shutil.get_terminal_size((80, 25)).columns, 120))
