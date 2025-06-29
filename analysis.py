from dataclasses import dataclass
from typing import Any
from scipy.stats import quantile_test

@dataclass(frozen=True)
class PathResults:
    path: Any
    stats: Any

    def __repr__(self):
        return f"PathResults({' > '.join(self.path)}: {self.stats})"

def analyze_runs(conn, baseline_start, baseline_end, mutant_start, mutant_end, span_path, percentile=0.99, significance_level=0.05):
    path_string = ' > '.join(span_path)
    print(f"Analyzing path {path_string}")

    baseline_df = fetch_times(conn, baseline_start, baseline_end, span_path)
    mutant_df = fetch_times(conn, mutant_start, mutant_end, span_path)

    baseline_quantile = baseline_df.quantile(percentile)
    test_results = quantile_test(mutant_df, q=baseline_quantile, p=percentile)

    print(f"Tested path {path_string} at {percentile=} with significance level {significance_level}: pvalue = {test_results.pvalue}")
    if test_results.pvalue < significance_level:
        baseline_child_spans = set(fetch_child_spans(conn, baseline_start, baseline_end, span_path).to_list())
        mutant_child_spans = set(fetch_child_spans(conn, mutant_start, mutant_end, span_path).to_list())
        child_spans = set.intersection(baseline_child_spans, mutant_child_spans)

        if (baseline_child_spans or mutant_child_spans) and not child_spans:
            print(f"Warning: found no common child span for path {path_string} between baseline and mutant!")

        child_results = []
        for child in child_spans:
            child_span = span_path + [child]
            child_results.extend(analyze_runs(conn, baseline_start, baseline_end, mutant_start, mutant_end, child_span))

        if not child_results:
            return [PathResults(span_path, test_results)]
        else:
            return child_results
    else:
        return []

def fetch_child_spans(conn, start, end, span_path):
    match_spans = []
    n = len(span_path) + 1
    for i in range(n):
        match_spans.append(f"(s{i + 1}: Span)")
    match_clause = '-[:HasChild]->'.join(match_spans)

    where_spans = []
    for i, span in enumerate(span_path):
        where_spans.append(f"s{i + 1}.name = '{span}'")
    where_clause = ' AND '.join(where_spans)

    query = f"""
        MATCH {match_clause}
        WHERE {where_clause} AND s{n}.timestamp >= timestamp('{start}') AND s{n}.timestamp <= timestamp('{end}')
        RETURN DISTINCT s{n}.name AS name;
    """

    return conn.execute(query).get_as_pl().get_column("name")


def fetch_times(conn, start, end, span_path):
    match_spans = []
    n = len(span_path)
    for i in range(n):
        match_spans.append(f"(s{i + 1}: Span)")
    match_clause = '-[:HasChild]->'.join(match_spans)

    where_spans = []
    for i, span in enumerate(span_path):
        where_spans.append(f"s{i + 1}.name = '{span}'")
    where_clause = ' AND '.join(where_spans)

    query = f"""
        MATCH {match_clause}
        WHERE {where_clause} AND s{n}.timestamp >= timestamp('{start}') AND s{n}.timestamp <= timestamp('{end}')
        RETURN s{n}.duration_us AS duration_us;
    """

    return conn.execute(query).get_as_pl().get_column("duration_us")