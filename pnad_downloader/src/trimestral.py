from src import BASE_PATH, PnadReadVars
from src.utils import lpad

def build_pnad_trimestral(year:int, quarter:int) -> PnadReadVars:
    return PnadReadVars(
        download_file_abspath = BASE_PATH + "Trimestral/Microdados/" + str(year) + "/",
        download_zipped_file_re = f"^PNADC_{lpad(str(quarter), 2, '0')}{str(year)}",
        download_file_re = f".*\\.txt",
        input_file_abspath = BASE_PATH + "Trimestral/Microdados/Documentacao/",
        input_is_zipped = True,
        input_zipped_file_re = "^Dicionario_e_input",
        input_file_re = "^input.*\\.txt",
        dictionary_file_abspath = BASE_PATH + "Trimestral/Microdados/Documentacao/",
        dictionary_is_zipped = True,
        dictionary_zipped_file_re = "^Dicionario_e_input",
        dictionary_file_re = "^dicionario.*\\.xls",
        save_filename = f"pnad_trimestral_{str(year)}_trimestre_{str(quarter)}"
    )
