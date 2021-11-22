#!/usr/bin/python3
import argparse
from functools import partial
import logging

from src import CONVERT_ASCII
from src.reader import PnadReader
from src.utils import get_list_size, apply_inplace
from src.anual import build_pnad_anual_trimestre, build_pnad_anual_visita
from src.trimestral import build_pnad_trimestral

class WrongArgument(Exception):
    pass

def parse_args():
    parser = argparse.ArgumentParser(description="Baixa, transforma e salva os dados da PNADC")
    parser.add_argument('-y', '--year', dest='year', type=int, help="Ano da PNADC", required=True)
    parser.add_argument('-q','--quarter', dest='quarter', type=int, default=None, help="Trimestre da PNADC")
    parser.add_argument('-v','--visit', dest='visit', type=int, default=None, help="Número da visita")
    parser.add_argument('-a','--anual', dest='force_yearly', action="store_true", default=False, help="Forçar dados anuais (útil para trimestres)")
    parser.add_argument('-t','--translate', dest='translate', action="store_true", default=False, help="Traduzir códigos para a forma legível")
    parser.add_argument('-n','--col-names', dest='col_names', action="store_true", default=False, help="Substituir código das variáveis pelas descrições correspondentes")
    args = parser.parse_args()
    logging.debug(f"""Parsed args:
    Year: {args.year}
    Quarter: {args.quarter}
    Visit: {args.visit}
    Translate: {args.translate}
    Force yearly: {args.force_yearly}
    Change column names: {args.col_names}""")
    return args

def main():
    reader = None
    args = parse_args()
    if args.visit and args.quarter:
        raise WrongArgument("Only one of 'Trimestre' or 'Visita' must be passed")
    if args.visit:
        reader = PnadReader(build_pnad_anual_visita(args.year, args.visit))
    elif args.quarter:
        if args.force_yearly:
            reader = PnadReader(build_pnad_anual_trimestre(args.year, args.quarter))
        else:
            reader = PnadReader(build_pnad_trimestral(args.year, args.quarter))
    else:
        raise WrongArgument("'Trimestre' or 'Visita' must be passed")

    logging.info("Downloading file")
    pnad = reader.download_pnad()
    logging.info(f"Data size: {get_list_size(pnad) / (1024 * 1024)} MB")
    logging.info("Parsing and transforming data")
    pnad_data = map(reader.parse_row, pnad)
    if args.translate:
        translate = partial(reader.translate_record, tvars = reader.pnad_vars)
        pnad_data = map(translate, pnad_data)
    else:
        # Converts only state and capital values
        translate = partial(reader.translate_record, tvars = ["UF","Capital","RM_RIDE"])
        pnad_data = map(translate, pnad_data)
    if args.col_names:
        pnad_data = map(reader.var_to_name_record, pnad_data)
    if CONVERT_ASCII:
        pnad_data = map(reader.to_ascii, pnad_data)
    pnad_data = list(pnad_data)
    # Insert a column with the UF abreviations
    apply_inplace(pnad_data, reader.insert_uf_abrev_from_name)
    logging.info("Saving to file")
    reader.to_file(pnad_data)
    

if __name__ == "__main__":
    main()