import re
from builtins import set

import editdistance
import spacy
from elasticsearch import Elasticsearch

from customizations.constants import CLASS_INDEX, RELATION_INDEX
from main import search_props_and_entities
from customizations.model.core import LinkedCandidate
from customizations.model.core import LinkingResponse
from customizations.sparql.query_helper import get_rdf_types

nlp = spacy.load('en_core_web_sm')
es = Elasticsearch(hosts=['http://geo-qa.cs.upb.de:9200/'])

# TODO
# 1 remove classes based on substring
# 2 remove candidates based on length > 3 or number regex

def process_input(question):
    classes = process_text_C(question)
    entities, relations = search_props_and_entities(question)
    relations = process_text_R(question)

    # set searchTerm as originalTerm for entities and relations
    for e in entities:
        e.append(e[4])

    # enrich
    linked_classes = [LinkedCandidate.from_value_array(x) for x in classes]
    linked_relations = [LinkedCandidate.from_value_array(x) for x in relations]
    linked_entities = [LinkedCandidate.from_value_array(x) for x in entities]

    # mark start and end of search term in the question
    linked_classes = [mark_start_end_index(question, x) for x in linked_classes if x.levensteinDistance < 4]
    linked_relations = [mark_start_end_index(question, x) for x in linked_relations if x.levensteinDistance < 4]
    linked_entities = [mark_start_end_index(question, x) for x in linked_entities if x.levensteinDistance < 4]

    # Remove relations for which the search term has already given a class link
    class_search_terms = []
    for class_link in linked_classes:
        split = class_link.searchTerm.split(" ")
        class_search_terms.extend(split)
    linked_relations = [relation_link for relation_link in linked_relations if
                        relation_link.searchTerm not in class_search_terms]

    # add types for entities
    entity_uris = [x.uri for x in linked_entities]
    entity_rdf_types = get_rdf_types(entity_uris)
    for le in linked_entities:
        if le.uri in entity_rdf_types:
            le.types = list(entity_rdf_types[le.uri])

    return LinkingResponse(question, linked_classes, linked_relations, linked_entities)


def mark_start_end_index(question: str, linked_candidate: LinkedCandidate):
    term_index = [m.start() for m in re.finditer(linked_candidate.originalTerm, question)]
    if len(term_index) == 0:
        term_index = [m.start() for m in re.finditer(linked_candidate.originalTerm, question, re.IGNORECASE)]

    linked_candidate.startIndex = term_index
    return linked_candidate


def process_text_C(question):
    # print(question)
    doc = nlp(question)
    results = []

    search_candidates = []
    original_terms = {}
    for term in doc:
        if term.pos_ == "NOUN":
            search_candidates.append(term.lemma_)
            search_candidates.append(doc[term.i - 1].text + " " + term.lemma_)
            original_terms[term.lemma_] = term.text
            original_terms[doc[term.i - 1].text + " " + term.lemma_] = doc[term.i - 1:term.i + 1].text

    for search_term in search_candidates:
        search_results = classSearch(search_term)
        if len(search_results) > 0:
            for sr in search_results:
                sr.append(original_terms.get(search_term))
            results.extend(search_results)

    results = sorted(results, key=lambda x: (x[1], -x[3], -x[2]))
    seen = set()
    results = [x for x in results if x[1] not in seen and not seen.add(x[1])]
    results = sorted(results, key=lambda x: (-x[3], -x[2], x[1]))  # NOTE Enhancement

    return results


def process_text_R(question):
    doc = nlp(question)
    results = []

    search_candidates = []
    for term in doc:
        if term.tag_ == "NN" and len(term.text) > 3:
            search_candidates.append(term.text)

    for search_term in search_candidates:
        search_results = relationSearch(search_term)
        if len(search_results) > 0:
            for sr in search_results:
                sr.append(search_term)
            results.extend(search_results)

    results = sorted(results, key=lambda x: (x[1], -x[3], -x[2]))
    seen = set()
    results = [x for x in results if x[1] not in seen and not seen.add(x[1])]
    results = sorted(results, key=lambda x: (-x[3], -x[2], x[1]))  # NOTE Enhancement

    return results


def classSearch(search_term):
    return indexSearch(search_term, CLASS_INDEX)


def relationSearch(search_term):
    return indexSearch(search_term, RELATION_INDEX)


def indexSearch(search_term, index):
    results = []
    ###################################################
    elasticResults = es.search(index=index, body={
        "query": {
            "match": {"label": search_term}
        }
        , "size": 100
    }
                               )
    for result in elasticResults['hits']['hits']:
        if result["_source"]["label"].lower().replace('.', '').strip() == search_term.lower().strip():
            results.append(
                [result["_source"]["label"], result["_source"]["uri"], result["_score"] * 50, 40, search_term])
        else:
            results.append(
                [result["_source"]["label"], result["_source"]["uri"], result["_score"] * 40, 0, search_term])

    ###################################################
    elasticResults = es.search(index=index, body={
        "query": {
            "match": {
                "label": {
                    "query": search_term,
                    "fuzziness": "AUTO"

                }
            }
        }, "size": 100
    }
                               )
    for result in elasticResults['hits']['hits']:
        edit_distance = editdistance.eval(result["_source"]["label"].lower().replace('.', '').strip(),
                                          search_term.lower().strip())
        if edit_distance <= 1:
            results.append(
                [result["_source"]["label"], result["_source"]["uri"], result["_score"] * 50, 40, search_term])
        else:
            results.append(
                [result["_source"]["label"], result["_source"]["uri"], result["_score"] * 25, 0, search_term])

    results = sorted(results, key=lambda x: (x[1][x[1].rfind("/") + 1:], -x[3], -x[2]))
    seen = set()
    results = [x for x in results if x[1] not in seen and not seen.add(x[1])]
    # results = results[:20]
    results = sorted(results, key=lambda x: (-x[3], -x[2], x[1][x[1].rfind("/") + 1:]))

    # TODO take all top ranked results instead of just one
    return results[:1]


def read_benchmark_questions():
    with open("/home/hardik/Projects/falcon2.0/customizations/geo-qa-benchmark.json", "r") as benchmark_file:
        import json
        benchmark_json = json.load(benchmark_file)
        questions_json = benchmark_json["questions"]
        questions_list = []
        for question in questions_json:
            questions_list.append(question["question"][0]["string"])
        return questions_list


if __name__ == "__main__":
    # "/home/hardik/Projects/falcon2.0/customizations/lgdo_2014-07-26.n3"
    questions_list = read_benchmark_questions()

    for q in questions_list:
        # print(q)

        # Find classes
        # classes = process_text_C(q)
        # for result in classes:
        #     print(result)

        # Find entities
        # entities = search_props_and_entities(q)
        # for e in entities[0]:
        #     print(e)

        linking_response = process_input(q)
        print(linking_response.input_str)
        print([f"{x.searchTerm}{x.startIndex}={x.label}" for x in linking_response.linkedClasses])
        print([f"{x.searchTerm}{x.startIndex}={x.label}" for x in linking_response.linkedRelations])
        print([f"{x.searchTerm}{x.startIndex}={x.label}" for x in linking_response.linkedEntities])

        print()
        # break
