#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2018, Silvio Peroni <essepuntato@gmail.com>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

from argparse import ArgumentParser
from requests import get
from urllib.parse import quote
from time import sleep
from json import loads, load
from csv import DictReader, DictWriter, writer
from os.path import exists, sep
from collections import OrderedDict, deque
from re import findall
from glob import glob

AGENT_NAME = "getdata.py"
USER_AGENT = "%s (SoS Gang - https://sosgang.github.io; mailto:essepuntato@gmail.com)" % AGENT_NAME
MAX_TENTATIVE = 5
ERROR = "E"
WARNING = "W"
INFO = "I"
COCI_API = "https://w3id.org/oc/index/coci/api/v1/citations/"
CROSSREF_API = "https://api.crossref.org/works/"
JOURNAL_NUMBER_DATE_WINDOW = {
    1: 10,
    2: 5
}
CITATION_NUMBER_DATE_WINDOW = {
    1: 15,
    2: 10
}


def norm(doi):
    return doi.strip().lower()


def call_api(doi, api_url=COCI_API):
    result = None
    cur_tentative = 0

    if doi.strip() != "":
        while result is None and cur_tentative < MAX_TENTATIVE:
            cur_tentative += 1
            try:
                r = get(api_url + quote(doi),
                        headers={"User-Agent": USER_AGENT}, timeout=30)
                if r.status_code == 200:
                    r.encoding = "utf-8"
                    result = loads(r.text)
            except Exception as e:
                sleep(10)

    return result


def as_list(s):
    return [s]


def retrieve_doi_from_file(f_path, header, delimiter=",", func=as_list):
    result = set()

    csv_data = open_csv_file(f_path, delimiter)
    if csv_data:
        for row in csv_data:
            if row[header] is not None:
                result.update([norm(doi) for doi in func(row[header])])

    return result


def open_csv_file(f_path, delimiter=","):
    try:
        with open(f_path) as f:
            return list(DictReader(f, delimiter=delimiter))
    except Exception:
        return []


def split_by_comma(s):
    return s.split(", ")


def prtmes(fun, mes_type, text):
    print("{%s} [%s] %s" % (fun, mes_type, text))


def same_object(o, doi):
    return o


def get_type_date(o, doi):
    if o:
        return [{"doi": doi, "type": o["message"]["type"], "date": datestrings(o["message"])}]
    else:
        return [{"doi": doi, "type": "not-defined", "date": "not-defined"}]


def datestrings(o):
    result = "not-defined"

    if "issued" in o and "date-parts" in o["issued"] and o["issued"]["date-parts"][0][0]:
        tmp_list = []
        for i in o["issued"]["date-parts"][0]:
            i_str = str(i)
            if len(i_str) == 1:
                i_str = "0" + i_str
            tmp_list.append(i_str)
        result = "-".join(tmp_list)

    return result


def extract_data_from_doi(i, o, api_url, res_func, header, no_doi_f, dup_dois_f,
                          input_field, input_function, input_separator,
                          output_field, output_function, output_separator):
    output_file_exist = exists(o)
    output_doi_file_exists = exists(no_doi_f) if no_doi_f else False
    doi_to_parse = retrieve_doi_from_file(i, input_field, input_separator, input_function)
    doi_already_retrieved = retrieve_doi_from_file(o, output_field, output_separator, output_function)
    no_doi = retrieve_doi_from_file(no_doi_f, "doi")

    original_duplicate = {}
    if dup_dois_f:
        dup_dois = open_csv_file(dup_dois_f)
        for row in dup_dois:
            doi_1 = norm(row["doi_1"])
            doi_2 = norm(row["doi_2"])
            if doi_1 in doi_to_parse and doi_2 not in doi_to_parse:
                original_duplicate[doi_2] = doi_1
            elif doi_1 not in doi_to_parse and doi_2 in doi_to_parse:
                original_duplicate[doi_1] = doi_2
            else:
                prtmes("extract_data_from_doi", WARNING, "Duplicated DOIs '%s' and '%s' in the input file" %
                       (doi_1, doi_2))
                original_duplicate[doi_2] = doi_1

    # Extends the DOIs to parse with the duplicates if any
    doi_to_parse.update(original_duplicate.keys())

    for doi in doi_to_parse.difference(doi_already_retrieved).difference(no_doi):
        res = res_func(call_api(doi, api_url), doi)
        if res:
            prtmes("extract_data_from_doi", INFO, "Get information for DOI '%s'" % doi)

            if doi in original_duplicate:
                for row in res:
                    row[output_field] = original_duplicate[doi]

            # Store new data
            with open(o, "a") as f:
                wr = DictWriter(f, header, extrasaction="ignore")

                if not output_file_exist:
                    wr.writeheader()
                    output_file_exist = True
                wr.writerows(res)
        else:
            prtmes("extract_data_from_doi", INFO, "No result for DOI '%s'" % doi)

        # Store new DOI
        if no_doi_f:
            with open(no_doi_f, "a") as f:
                wr = writer(f)

                if not output_doi_file_exists:
                    wr.writerow(("doi", ))
                    output_doi_file_exists = True
                wr.writerow((doi,))


def extract_citations(i, o, no_doi, dup_dois):
    prtmes("extract_citations", INFO, "Run the extraction of citations.")
    extract_data_from_doi(i, o, COCI_API, same_object, ("citing", "cited", "creation"), no_doi, dup_dois,
                          "dois", split_by_comma, "\t",
                          "cited", as_list, ",")
    prtmes("extract_citations", INFO, "The extraction of citations has been completed.")


def retrieve_entity_types(i, o, dup_dois):
    prtmes("retrieve_entity_types", INFO, "Run the identification of entity types.")
    extract_data_from_doi(i, o, CROSSREF_API, get_type_date, ("doi", "type", "date"), None, dup_dois,
                          "dois", split_by_comma, "\t",
                          "doi", as_list, ",")
    prtmes("retrieve_entity_types", INFO, "The identification of entity types has been completed.")


def transform_in_json_by_key(data, key, func=norm):
    result = {}

    for row in data:
        value = func(row[key])
        if value not in result:
            result[value] = []
        result[value].append(row)

    return result


def get_duplicated_doi_table(duplicated_doi_f):
    result = {}

    if duplicated_doi_f:
        for row in open_csv_file(duplicated_doi_f):
            doi1 = norm(row["doi_1"])
            doi2 = norm(row["doi_2"])
            result[doi1] = doi2
            result[doi2] = doi1

    return result


def calculate_journal_number(row, data, session_date, duplicated_doi_f):
    result = 0
    level = int(row["fascia"])
    base_date = str(int(session_date[:4]) - JOURNAL_NUMBER_DATE_WINDOW[level])

    duplicated_doi = get_duplicated_doi_table(duplicated_doi_f)

    if row["dois"]:
        for doi in [norm(doi) for doi in row["dois"].split(", ")]:
            el = data.get(doi)
            if el is None and doi in duplicated_doi:
                el = data.get(duplicated_doi[doi])

            if el and el[0]["type"] == "journal-article" and base_date <= el[0]["date"] <= session_date:
                result += 1

    return result


def calculate_citations_per_article(row, dates, citations, session_date, duplicated_doi_f):
    result = []

    level = int(row["fascia"])
    base_date = str(int(session_date[:4]) - CITATION_NUMBER_DATE_WINDOW[level])

    duplicated_doi = get_duplicated_doi_table(duplicated_doi_f)

    if row["dois"]:
        for doi in [norm(doi) for doi in row["dois"].split(", ")]:
            el = dates.get(doi)
            if el is None and doi in duplicated_doi:
                el = dates.get(duplicated_doi[doi])

            if base_date <= el[0]["date"] <= session_date:
                valid_citations = 0

                all_citing = set()
                all_citations = []
                if doi in citations:
                    for citation in citations[doi]:
                        all_citing.add(citation["citing"])
                        all_citations.append(citation)

                if doi in duplicated_doi and duplicated_doi[doi] in citations:
                    for citation in citations[duplicated_doi[doi]]:
                        if citation["citing"] not in all_citing:
                            all_citations.append(citation)

                for citation in all_citations:
                    creation_date = citation["creation"]
                    if creation_date and base_date <= creation_date <= session_date:
                        valid_citations += 1

                result.append(valid_citations)

    return result


def calculate_citation_number(row, dates, citations, session_date, duplicated_doi):
    result = calculate_citations_per_article(row, dates, citations, session_date, duplicated_doi)
    return sum(result)


def calculate_h_index(row, dates, citations, session_date, duplicated_doi):
    result = 0
    cits = calculate_citations_per_article(row, dates, citations, session_date, duplicated_doi)
    for i, n in enumerate(sorted(cits, reverse=True)):
        if i <= n:
            result = i + 1
        else:
            break
    return result


def identity(o):
    return o


def calculate_values(i, citations, types_and_dates, threshold_file, session_dates, duplicated_dois, o):
    prtmes("calculate_values", INFO, "Run the calculation of all the values for the evaluation.")
    result = []

    citation_data = transform_in_json_by_key(open_csv_file(citations, ","), "cited")
    type_and_date_data = transform_in_json_by_key(open_csv_file(types_and_dates, ","), "doi")
    thresholds = transform_in_json_by_key(open_csv_file(threshold_file, ","), "id", func=identity)

    for row in open_csv_file(i, "\t"):
        prtmes("calculate_values", INFO, "Retrive figures for %s %s" % (row["name"], row["surname"]))
        session_date = session_dates.split(" ")[int(row["sessione"]) - 1]
        participant_id = row["id-dblp"]

        result.append(
            {
                "id": participant_id,
                "given_name": row["name"],
                "family_name": row["surname"],
                "level": row["fascia"],
                "session_date": session_date,
                "journal_number_open": calculate_journal_number(row, type_and_date_data, session_date,
                                                                duplicated_dois),
                "citation_number_open": calculate_citation_number(row, type_and_date_data, citation_data,
                                                                  session_date, duplicated_dois),
                "h_index_open": calculate_h_index(row, type_and_date_data, citation_data, session_date,
                                                  duplicated_dois),
                "journal_number_real": thresholds[participant_id][0]["journal"],
                "citation_number_real": thresholds[participant_id][0]["citations"],
                "h_index_real": thresholds[participant_id][0]["h_index"]
            }
        )

    write_csv_file(result, o, ("id", "given_name", "family_name", "level", "session_date",
                               "journal_number_open", "citation_number_open", "h_index_open",
                               "journal_number_real", "citation_number_real", "h_index_real"))
    prtmes("calculate_values", INFO, "The calculation of all the values for the evaluation has been completed.")


def write_csv_file(data, file_path, header, way="w"):
    with open(file_path, way) as f:
        writer = DictWriter(f, header)
        if way == "w" or (way == "a" and not exists(file_path)):
            writer.writeheader()
        writer.writerows(data)


def merge_types_and_dates(instruction_file_patj, out_file_path):
    prtmes("merge_types_and_dates", INFO, "Run the merging of types and dates of articles.")
    result = []

    with open(instruction_file_patj) as f:
        instruction_json = load(f)
        main_key = instruction_json["key"]
        all_fields = instruction_json["header"]
        all_fields.remove(main_key)

        files = OrderedDict()
        all_dois = set()
        for key, file_path in instruction_json["files"]:
            files[key] = transform_in_json_by_key(open_csv_file(file_path), main_key)
            all_dois.update(retrieve_doi_from_file(file_path, main_key))

        doi_already_retrieved = retrieve_doi_from_file(out_file_path, "doi", ",", as_list)

        for doi in all_dois.difference(doi_already_retrieved):
            row = {main_key: doi}
            # Building dictionary with the values in consideration
            values = {}
            for key in files:
                values[key] = {}
                for field in all_fields:
                    cur_data = files[key].get(doi)
                    if cur_data:
                        values[key][field] = cur_data[0][field]
                    else:
                        values[key][field] = "not-defined"

            # Compare and select
            for field in all_fields:
                precedence_main = instruction_json["precedence"][field]["main"]
                selected_value = values[precedence_main][field]
                if selected_value == "not-defined":
                    alternatives = deque(files.keys())
                    alternatives.remove(precedence_main)
                    while selected_value == "not-defined" and alternatives:
                        alternative = alternatives.popleft()
                        selected_value = values[alternative][field]
                else:
                    for s, v in instruction_json["precedence"][field]["exceptions"]:
                        if values[s][field] == v:
                            selected_value = v

                mappings = instruction_json["mapping"].get(field)
                if mappings:
                    selected_value = mappings[selected_value] if selected_value in mappings else selected_value

                row[field] = selected_value

            result.append(row)

    write_csv_file(result, out_file_path, [main_key] + all_fields, "a")
    prtmes("merge_types_and_dates", INFO, "The merging of all the types and dates of articles has been completed.")


def get_thresholds(i, o, material_path):
    prtmes("get_thresholds", INFO, "Run the retrieving of personal thresholds.")
    result = []

    candidates = open_csv_file(i, "\t")

    for candidate in candidates:
        num, fn, gn = findall("^([0-9]+)_([A-Z_']+)_(.+)$", candidate["cv filename"][:-4])[0]
        local_file_name = "%s-%s_%s" % (num, fn.replace("_", " "), gn.replace("_", " "))

        html_string = None
        try:
            file_path = "%s%sfascia%s%ssessione%s_01-B1%s%s_indicatori.html" % \
                        (material_path, sep, candidate["fascia"], sep, candidate["sessione"], sep, local_file_name)
            with open(file_path) as f:
                html_string = f.read()
        except:
            file_path = "%s%sfascia%s%ssessione%s_01-B1%s*_%s.Indicatori.html" % \
                        (material_path, sep, candidate["fascia"], sep, candidate["sessione"],
                         sep, local_file_name.split("-")[1])
            file_list = glob(file_path)
            if len(file_list) == 1:
                with open(file_list[0]) as f:
                    html_string = f.read()
            else:
                print("ISSUE!", file_path, file_list)

        values = findall("<tr>\n\s+<td class=\"text-center\">([0-9]+)</td>\n\s+<td class=\"text-center\">"
                         "([0-9]+)</td>\n\s+<td class=\"text-center\">([0-9]+)</td>\n\s+</tr>", html_string)[0]
        result.append({
            "id": candidate["id-dblp"],
            "journal": values[0],
            "citations": values[1],
            "h_index": values[2]
        })

    write_csv_file(result, o, ("id", "journal", "citations", "h_index"))
    prtmes("get_thresholds", INFO, "The retrieving of personal thresholds has been completed.")


if __name__ == "__main__":
    arg_parser = ArgumentParser("getdata.py", description="This script allows one to get the information about the "
                                                          "citations received by the authors, according to the DOI "
                                                          "of the articles they have published.")

    arg_parser.add_argument("-i", "--input", required=True,
                            help="The input file containing the initial information.")
    arg_parser.add_argument("-o", "--output", required=True,
                            help="The CSV file containing where to store the information.")
    arg_parser.add_argument("-ec", "--extract_citations", action="store_true", default=False,
                            help="Run the extraction of citations from DOI contained in the input file.")
    arg_parser.add_argument("-ret", "--retrieve_entity_types", action="store_true", default=False,
                            help="Run the retrieving of the various entities described by the DOI in the input file "
                                 "by using Crossref as source dataset.")
    arg_parser.add_argument("-m", "--merge_types_and_dates", action="store_true", default=False,
                            help="Run the merging of the files containing types and dates specified in the "
                                 "instructions provided as input, and store the merged material in the output.")
    arg_parser.add_argument("-cv", "--calculate_values", action="store_true", default=False,
                            help="Run the calculation of all the values of the authors in the input file.")
    arg_parser.add_argument("-sd", "--session_dates", default=None,
                            help="The dates of all the sessions considered.")
    arg_parser.add_argument("-thf", "--threshold_path", default=None,
                            help="The directory where to retrieve of all the thresholds of the candidates "
                                 "specified in the input file, when called alone, or the file containing "
                                 "the thresholds when called with '-cv'.")
    arg_parser.add_argument("-tf", "--type_file", default=None,
                            help="The CSV file containing types and date of publications "
                                 "(to be used in presence of '-cv'.")
    arg_parser.add_argument("-cf", "--citation_file", default=None,
                            help="The CSV file containing the citations of publications. "
                                 "(to be used in presence of '-cv'.")
    arg_parser.add_argument("-nd", "--no_doi", default=None,
                            help="The file containing the list of DOI which should not be "
                                 "considered in the operation.")
    arg_parser.add_argument("-dd", "--duplicated_doi", default=None,
                            help="The CSV file containing publications with two DOIs assigned. "
                                 "(to be used in presence of '-ec'.")

    # Session dates ASN 2016-2018: 2016-12-02 2017-04-03 2017-08-04 2017-12-05 2018-04-06

    args = arg_parser.parse_args()

    if args.extract_citations:
        extract_citations(args.input, args.output, args.no_doi, args.duplicated_doi)
    elif args.retrieve_entity_types:
        retrieve_entity_types(args.input, args.output, args.duplicated_doi)
    elif args.calculate_values and args.type_file and args.citation_file and \
            args.session_dates and args.threshold_path:
        calculate_values(args.input, args.citation_file, args.type_file, args.threshold_path,
                         args.session_dates, args.duplicated_doi, args.output)
    elif args.merge_types_and_dates:
        merge_types_and_dates(args.input, args.output)
    elif args.threshold_path:
        get_thresholds(args.input, args.output, args.threshold_path)