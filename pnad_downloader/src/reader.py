from collections import defaultdict
from functools import partial
from itertools import chain, repeat
import logging
import struct
from typing import Any, Callable, Dict, List, Sequence, Tuple, Generator
import zipfile
import ftplib
import socket
import io
import re
import os
import csv
import xlrd

from src import SAVE_PATH, MAX_LINES, PnadParse, PnadDict, PnadData, PnadInputInfo, PnadReadVars, Record
from src.utils import ASCII_CHARS, STATES_ABREV, STATES_ABREV_ASCII_NAMES, chunks, get_list_size, lpad, maybe_int, standardize, uniques_from_list

def zipfile_lines_by_path(abspath:str, zipfile_re:str, file_re:str, ftp:ftplib.FTP) -> Generator[bytes, None, None]:
    """Yields lines of unzipped file from ftp server

    Args:
        abspath (str): absolute path to file in the FTP server
        zipfile_re (str): lowercase regex to match with the .zip file
        file_re (str): lowercase regex to match with the file to unzip
        ftp (ftplib.FTP): a FTP object with the connection to the FTP server

    Yields:
        Generator[bytes, None, None]: A Generator of the lines of the file as byte strings
    """
    ftp.cwd(abspath)
    match = partial(re.match, zipfile_re)
    data_file = next(filter(match, ftp.nlst()))
    with io.BytesIO() as zipdata:
        ftp.retrbinary(f"RETR {data_file}", zipdata.write)
        zipped_file = zipfile.ZipFile(zipdata)
        match = partial(re.match, file_re)
        file_name = next(filter(match, map(lambda x:x.filename, zipped_file.filelist)))
        with zipped_file.open(file_name, 'r') as opened_file:
            for line in opened_file:
                yield line

def zipfile_contents_by_path(abspath:str, zipfile_re:str, file_re:str, ftp:ftplib.FTP) -> bytes:
    """Returns an unzipped file contents as bytes

    Args:
        abspath (str): absolute path to zipped file in the FTP server
        zipfile_re (str): regex to match with the .zip file
        file_re (str): regex to match with the file to unzip
        ftp (ftplib.FTP): a FTP object with the connection to the FTP server

    Returns:
        bytes: The unzipped file contents read as bytes
    """
    ftp.cwd(abspath)
    match = partial(re.match, zipfile_re)
    data_file = next(filter(match, ftp.nlst()))
    with io.BytesIO() as zipdata:
        ftp.retrbinary(f"RETR {data_file}", zipdata.write)
        zipped_file = zipfile.ZipFile(zipdata)
        match = partial(re.match, file_re)
        file_name = next(filter(match, map(lambda x:x.filename, zipped_file.filelist)))
        with zipped_file.open(file_name, 'r') as opened_file:
            return opened_file.read()


def file_contents_by_path(abspath:str, file_re:str, ftp:ftplib.FTP) -> bytes:
    """Return an entire file retrieved fro FTP server

    Args:
        abspath (str): absolute path to file directory
        file_re (str): lowercase regex to match with file to retrieve
        ftp (ftplib.FTP): a FTP object containing the connection to the remote server

    Return:
        bytes: The file read as bytes
    """
    ftp.cwd(abspath)
    match = partial(re.match, file_re)
    data_file = next(filter(match, ftp.nlst()))
    with io.BytesIO() as datafile:
        ftp.retrbinary(f"RETR {data_file}", datafile.write)
        datafile.seek(0)
        return datafile.read()

class PnadVariableNotFound(Exception):
    """Exception to be raised when pnad variable is not found"""
    pass

def xls_to_pnad_dict(pnad_dict:List[List[Any]], 
    input_info:PnadInputInfo) -> PnadDict:
    """Converts the dictionary file as a list of lines to a PnadDict

    Args:
        pnad_dict (List[List[str]]): the xls file downloaded and separated by cells
        input_info (PnadInputInfo): the input file info to be used

    Raises:
        PnadVariableNotFound: could not find current variable in input file information

    Returns:
        PnadDict: the dictionary information as a PnadDict
    """
    d : Dict[str, Dict[str, str]] = defaultdict(dict)
    current_var = ''

    # For each variable, store the codes and respective values
    # ----------- Do NOT transform! -----------
    special_cases = ['Ano', 'Trimestre', 'UPA', 'Estrato', 'V1008', 'V1014', 'V1016', 'V1029', 'posest']

    # --------- For all the remaining ---------
    for i in range(6, len(pnad_dict)): # Begin after "Trimestre" line
        if pnad_dict[i][2]:
            current_var : str = pnad_dict[i][2] # Store the current variable name
        if current_var in special_cases:
            continue # Just jump to the next line
        if isinstance(pnad_dict[i][6], str):
            if not pnad_dict[i][5]: # If the code is blank, probably variable does not apply
                if pnad_dict[i][6]:
                    try:
                        real_len = next(filter(lambda x:x[1] == current_var, input_info))[2]
                        d[current_var].update({''.join(repeat(' ', real_len)) : pnad_dict[i][6]})
                    except StopIteration:
                        raise PnadVariableNotFound(f"Could not found variable {current_var} on input file info")
            elif not maybe_int(pnad_dict[i][5]): 
                if not current_var in d.keys():
                    d[current_var] = dict() # Just put an empty dict
            else:
                try:
                    real_len = next(filter(lambda x:x[1] == current_var, input_info))[2]
                    num_str = lpad(str(int(pnad_dict[i][5])), real_len, '0')
                    d[current_var].update({num_str : pnad_dict[i][6]})
                except StopIteration:
                    raise PnadVariableNotFound(f"Could not found variable {current_var} on input file info")

    # ---------- Add the exceptions ----------
    for var in special_cases:
        d[var] = dict()
    
    # Return all variables in order
    return PnadDict({var:d[var] for var in map(lambda ls:ls[1], input_info)})

def parser_cols_from_input(input_info:PnadInputInfo
    ) -> Tuple[Callable[[str],List[str]], List[str], List[str]]:
    """Get a parser for the strings of pnad file and the column names from input file"""
    fieldstruct = struct.Struct(' '.join(map(lambda x:str(x[2]) + 's', input_info)))
    unpack = fieldstruct.unpack_from
    parse: Callable[[str], List[str]] = lambda line:[s.decode("utf-8") for s in unpack(line.encode("utf-8"))]
    columns: List[str] = [standardize(val[-1]) for val in input_info]
    variables: List[str] = [val[1] for val in input_info]
    return parse, columns, variables

def read_fieldnames(records:List[Record]) -> Sequence[str]:
    return uniques_from_list(chain(*list(map(lambda x:list(x.keys()), records))))

def records_to_csv(path:str, filename:str, records:List[Record]) -> None:
    """Saves records to spcified path and filename

    Args:
        path (str): the absolute path to the directory to save the file
        filename (str): the filename to be saved
        records (List[Record]): the data as a list of records
    """
    logging.debug(f"Parsed data size: {get_list_size(records) / (1024 * 1024)} MB")
    logging.debug("Reading fieldnames")
    fieldnames = read_fieldnames(records)
    logging.debug("Dividing in chunks")
    if MAX_LINES:
        records_chunks = chunks(records, MAX_LINES)
        del records
        counter = 0
        for rec_list in records_chunks:
            logging.info(f"Saving to {filename}_part_{counter}.csv")
            with open(str(os.path.join(path, f"{filename}_part_{counter}.csv")),'w') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for r in rec_list:
                    writer.writerow(r)
            counter += 1
    else:
        logging.info(f"Saving to {filename}.csv")
        with open(str(os.path.join(path, f"{filename}.csv")),'w') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in records:
                writer.writerow(r)


class PnadReader:
    """A class to download, parse, transform and save the pnad file data"""

    def __init__(self, pnad_reader_vars:PnadReadVars):
        """Initiates the PnadReader object

        Args:
            pnad_reader_vars (PnadReadVars): PnadReadVars object to be used for creation of PnadReader
        """
        self.pnad_read_vars = pnad_reader_vars
        self.input_info = self.download_input()
        self.pnad_dict = self.download_dictionary(self.input_info)
        self.parse, self.pnad_cols, self.pnad_vars = self.build_parser(self.input_info)

    @staticmethod
    def get_connection() -> ftplib.FTP:
        ftp = ftplib.FTP("ftp.ibge.gov.br")
        ftp.login()
        ftp.af = socket.AF_INET6
        return ftp

    def download_pnad(self) -> PnadData:
        """Downloads the pnad data file

        Returns:
            PnadData: the file lines as a list of strings
        """
        ftp = self.get_connection()
        file_abspath = self.pnad_read_vars.download_file_abspath
        zipped_file_re = self.pnad_read_vars.download_zipped_file_re
        file_re = self.pnad_read_vars.download_file_re
        pnad = [line.decode('ISO-8859-1') for line in zipfile_lines_by_path(file_abspath, zipped_file_re, file_re, ftp)]
        ftp.close()
        return PnadData(pnad)

    def download_input(self) -> PnadInputInfo:
        """Downloads the input file

        Returns:
            PnadInputInfo: input file in PnadInputInfo format
        """
        ftp = self.get_connection()
        file_abspath = self.pnad_read_vars.input_file_abspath
        input_info = list()
        if self.pnad_read_vars.input_is_zipped:
            zipped_file_re = self.pnad_read_vars.input_zipped_file_re
            file_re = self.pnad_read_vars.input_file_re
            with io.BytesIO() as input_file:
                input_file.write(zipfile_contents_by_path(file_abspath, zipped_file_re, file_re, ftp))
                input_file.seek(0)
                for line in input_file.readlines():
                    if line.decode('ISO-8859-1').startswith('@'):
                        line_data = list(filter(None,line.decode('ISO-8859-1').split()))
                        input_info.append((line_data[0], line_data[1], int(line_data[2].strip('$.')), ' '.join(line_data[4:-1]))) # Get positions, variable name and column name
        else:
            file_re = self.pnad_read_vars.input_file_re
            with io.BytesIO() as input_file:
                input_file.write(file_contents_by_path(file_abspath, file_re, ftp))
                input_file.seek(0)
                for line in input_file.readlines():
                    if line.decode('ISO-8859-1').startswith('@'):
                        line_data = list(filter(None,line.decode('ISO-8859-1').split())) 
                        input_info.append((line_data[0], line_data[1], int(line_data[2].strip('$.')), ' '.join(line_data[4:-1]))) # Get positions, variable name and column name
        
        return PnadInputInfo(input_info)

    def download_dictionary(self, input_info:PnadInputInfo) -> PnadDict:
        """Downloads the dictionary file and prepares it for use

        Args:
            input_info (PnadInputInfo): the input file iinformation to be used

        Returns:
            PnadDict: the dictionary file contents as a PnadDict
        """
        ftp = self.get_connection()
        file_abspath = self.pnad_read_vars.dictionary_file_abspath
        dict_lines = list()
        pnad_dict : PnadDict

        if self.pnad_read_vars.dictionary_is_zipped:
            zipped_file_re = self.pnad_read_vars.dictionary_zipped_file_re
            file_re = self.pnad_read_vars.dictionary_file_re
            dict_file: bytes = zipfile_contents_by_path(file_abspath, zipped_file_re, file_re, ftp)
        else:
            file_re = self.pnad_read_vars.dictionary_file_re
            dict_file: bytes = file_contents_by_path(file_abspath, file_re, ftp)
            
        dict_xls : xlrd.Book = xlrd.open_workbook(file_contents=dict_file, formatting_info=False)
        dict_sheet = dict_xls.sheet_by_index(0)
        for i in range(dict_sheet.nrows):
            dict_lines.append([x.value for x in dict_sheet.row(i)])
        pnad_dict = xls_to_pnad_dict(dict_lines, input_info)

        return pnad_dict

    def build_parser(self, input_info:PnadInputInfo) -> Tuple[PnadParse, List[str], List[str]] :
        """Creates the parser based on input file information and also the columns and variables

        Args:
            input_info (PnadInputInfo): the input file iinformation to be used
        
        Returns:
            Tuple[PnadParse, List[str], List[str]]: a tuple containing the parser, 
            the columns and variable names in the pnad file
        """
        parser : PnadParse
        columns : List[str]
        variables : List[str]
        parser, columns, variables = parser_cols_from_input(input_info)
        return parser, columns, variables

    def parse_row(self, row:str) -> Record:
        """Parses a single row from the pnad file

        Args:
            row (str): the row to parse as a decoded string

        Returns:
            Record: the parsed row as a Record
        """
        return Record({col:val for col, val in zip(self.pnad_vars, self.parse(row))})

    def translate_record(self, record:Record, tvars:List[str]) -> Record:
        """Translates codes to human readable strings

        Args:
            record (Record): the pnad parsed line as a record. 
            Keys must be the variable codes, not the descriptions
            tvars (List[str]): the variables to translate to string values

        Returns:
            Record: the translated record
        """
        d = dict()
        for variable, value in record.items():
            if variable in tvars:
                d.update({variable : self.pnad_dict[variable].get(value.replace('.',' '), value.strip(' .'))})
            else:
                d.update({variable : value.strip(' .')})
        return Record(d)

    def var_to_name_record(self, record: Record) -> Record:
        """Substitutes variables for their correspondent description"""
        return Record({col:val for col, val in zip(self.pnad_cols, record.values())})

    @staticmethod
    def to_ascii(record: Record) -> Record:
        """Converts record keys and values to ASCII

        Args:
            redord (Record): a record to format as ASCII

        Returns:
            Record: the new record
        """
        return Record({k.translate(ASCII_CHARS):v.translate(ASCII_CHARS) for k, v in record.items()})

    @staticmethod
    def insert_uf_abrev_from_number(record:Record):
        """Inserts inplace a new column containing the uf abreviations from UF codes

        Args:
            record (Record): a record to insert the new column
        """
        record.update({"UF_ABREV" : STATES_ABREV[record['UF']]})

    @staticmethod
    def insert_uf_abrev_from_name(record:Record) -> Record:
        """Inserts inplace a new column containing the uf abbreviations from UF names

        Args:
            record (Record): the record to insert
        """
        record.update({"UF_ABREV" : STATES_ABREV_ASCII_NAMES[record['UF'].lower().translate(ASCII_CHARS)]})

    def to_file(self, pnad:List[Record], save_path:str = SAVE_PATH) -> None:
        """Saves the pnad downloaded data to specified file

        Args:
            pnad (List[Record]): the parsed pnad data
        """
        records_to_csv(save_path, self.pnad_read_vars.save_filename, pnad)