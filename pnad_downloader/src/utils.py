from typing import Any, Callable, Iterable, List, Union
from functools import reduce
from itertools import repeat
import sys
import re

STATES_ABREV = {
    11 : 'RO',
    12 : 'AC',
    13 : 'AM',
    14 : 'RR',
    15 : 'PA',
    16 : 'AP',
    17 : 'TO',
    21 : 'MA',
    22 : 'PI',
    23 : 'CE',
    24 : 'RN',
    25 : 'PB',
    26 : 'PE',
    27 : 'AL',
    28 : 'SE',
    29 : 'BA',
    31 : 'MG',
    32 : 'ES',
    33 : 'RJ',
    35 : 'SP',
    41 : 'PR',
    42 : 'SC',
    43 : 'RS',
    50 : 'MS',
    51 : 'MT',
    52 : 'GO',
    53 : 'DF'
}

STATES_ABREV_ASCII_NAMES = {
    'rondonia' : 'RO',
    'acre' : 'AC',
    'amazonas' : 'AM',
    'roraima' : 'RR',
    'para' : 'PA',
    'amapa' : 'AP',
    'tocantins' : 'TO',
    'maranhao' : 'MA',
    'piaui' : 'PI',
    'ceara' : 'CE',
    'rio grande do norte' : 'RN',
    'paraiba' : 'PB',
    'pernambuco' : 'PE',
    'alagoas' : 'AL',
    'sergipe' : 'SE',
    'bahia' : 'BA',
    'minas gerais' : 'MG',
    'espirito santo' : 'ES',
    'rio de janeiro' : 'RJ',
    'sao paulo' : 'SP',
    'parana' : 'PR',
    'santa catarina' : 'SC',
    'rio grande do sul' : 'RS',
    'mato grosso do sul' : 'MS',
    'mato grosso' : 'MT',
    'goias' : 'GO',
    'distrito federal' : 'DF'
}

ASCII_CHARS = {
  ord('á') : 'a',
  ord('à') : 'a',
  ord('â') : 'a',
  ord('ó') : 'o',
  ord('ô') : 'o',
  ord('í') : 'i',
  ord('ã') : 'a',
  ord('ê') : 'e',
  ord('é') : 'e',
  ord('ç') : 'c',
  ord('ú') : 'u',
  ord('Á') : 'A',
  ord('À') : 'A',
  ord('Â') : 'A',
  ord('Ó') : 'O',
  ord('Ô') : 'O',
  ord('Í') : 'I',
  ord('Ã') : 'A',
  ord('Ê') : 'E',
  ord('É') : 'E',
  ord('Ç') : 'C',
  ord('Ú') : 'U'
}

def lpad(text:str, lenght:int, el_pad:str):
    return ''.join(list(repeat(el_pad, lenght - len(text)))) + text

def maybe_int(value:str) -> Union[int,None]:
    try:
        return int(value)
    except ValueError:
        return None

def uniques_from_list(ls:Iterable[Any]) -> List[Any]:
    """Returns unique values from list whilst preserving the order

    Args:
        ls (list): a list to return unique values from

    Returns:
        list: the unique values with preserved order
    """
    seen: set[Any] = set()
    seen_add = seen.add
    return [x for x in ls if not (x in seen or seen_add(x))]

def chunks(lst:List[Any], n:int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def get_list_size(ls:List[Any]) -> float:
    """Return size in memory of a list and all its elements"""
    return reduce(lambda x, y: x + y, (sys.getsizeof(v) for v in ls), 0) + sys.getsizeof(ls)

def standardize(mystring:str) -> str:
    """Utility to standardize column names for Databricks"""
    return re.sub(r"( |-|\.|\(|\))+","_", mystring).lower()

def merge_lines(line1: List[str], line2: List[str], indexes: List[int]) -> List[str]:
    """Keeps all values from line 1 that are in indexes,
    for the others, substitute by the value in line2

    Args:
        line1 (List[str]): pnad line to merge
        line2 (List[str]): ohter pnad line to merge
        indexes (List[int]): indexes of values to keep

    Returns:
        List[str]: merged pnad line
    """
    assert len(line1) == len(line2)
    return [a if idx in indexes else b for idx, a, b in zip(range(len(line1)), line1, line2)]

def apply_inplace(ls:List[Any], func:Callable[[Any], None]) -> None:
    """Applies an inplace function to all entries in list

    Args:
        ls (List[Any]): a list with entries to apply the function
        func (Callable[[Any], None]): the function to execute and return nothing
    """
    for l in ls:
        func(l)