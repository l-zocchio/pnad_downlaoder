from src import BASE_PATH, PnadReadVars

def build_pnad_anual_visita(year:int, visit:int) -> PnadReadVars:
    return PnadReadVars(
        download_file_abspath = BASE_PATH+ f'Anual/Microdados/Visita/Visita_{str(visit)}/Dados/',
        download_zipped_file_re =  f'^PNADC_{str(year)}.*\\.zip',
        download_file_re = f'.*\\.txt',
        input_file_abspath = BASE_PATH + f'Anual/Microdados/Visita/Visita_{str(visit)}/Documentacao/',
        input_is_zipped = False,
        input_file_re = f'^input_PNADC.*{str(year)}.*visita{str(visit)}.*\\.txt',
        dictionary_file_abspath = BASE_PATH + f'Anual/Microdados/Visita/Visita_{str(visit)}/Documentacao/',
        dictionary_is_zipped = False,
        dictionary_file_re = f'^dicionario_PNADC_microdados.*{str(year)}.*visita{str(visit)}.*\\.xls',
        save_filename = f"pnad_anual_{str(year)}_visita_{str(visit)}"
    )

def build_pnad_anual_trimestre(year:int, quarter:int) -> PnadReadVars:
    return PnadReadVars(
        download_file_abspath = BASE_PATH + f'Anual/Microdados/Trimestre/Trimestre_{str(quarter)}/Dados/',
        download_zipped_file_re = f'^PNADC_{str(year)}.*\\.zip',
        download_file_re = f'.*\\.txt',
        input_file_abspath = BASE_PATH + f'Anual/Microdados/Trimestre/Trimestre_{str(quarter)}/Documentacao/', 
        input_is_zipped = False,
        input_file_re = "^input_PNADC.*\\.txt",
        dictionary_file_abspath = BASE_PATH + f'Anual/Microdados/Trimestre/Trimestre_{str(quarter)}/Documentacao/',
        dictionary_is_zipped = False,
        dictionary_file_re = "^dicionario_PNADC.*\\.xls",
        save_filename = f"pnad_anual_{str(year)}_trimestre_{str(quarter)}"
    )