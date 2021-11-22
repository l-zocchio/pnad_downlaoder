from dataclasses import dataclass
import logging
from configparser import ConfigParser
from typing import Callable, Dict, List, NewType, Tuple
import os

BASE_PATH = "/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/"
SAVE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
if not os.path.isdir(SAVE_PATH):
    os.mkdir(SAVE_PATH)

config = ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'config.ini'))
CONVERT_ASCII = config['saving'].getboolean('convert_to_ascii', False)
MAX_LINES = config['saving'].getint('max_lines_per_file', None)
DEBUG = config['DEFAULT'].getboolean('debug', True)

if DEBUG:
    logging.basicConfig(level="DEBUG")
else:
    logging.basicConfig(level="INFO")
logging.debug("Program initiated")
logging.debug(f"""Configs: 
    Converto to ascii: {CONVERT_ASCII} 
    Max lines per file: {MAX_LINES}
    Truncate in multiple files: {bool(MAX_LINES)}""")

PnadDict = NewType('PnadDict', Dict[str, Dict[str, str]])
PnadInputInfo = NewType('PnadInputInfo', List[Tuple[str, str, int, str]])
PnadData = NewType('PnadData', List[str])
PnadParse = Callable[[str], List[str]]
Record = NewType('Record', Dict[str, str])

class ArgumentNotSpecifiedError(Exception):
    pass

@dataclass
class PnadReadVars:

    download_file_abspath: str
    download_zipped_file_re : str
    download_file_re : str
    input_file_abspath : str
    input_file_re : str
    dictionary_file_abspath : str
    dictionary_file_re : str
    save_filename : str
    input_is_zipped : bool = False
    input_zipped_file_re : str = ''
    dictionary_is_zipped : bool = False
    dictionary_zipped_file_re : str = ''