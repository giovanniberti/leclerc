from dataclasses import dataclass
from typing import Any
import scipy.stats

@dataclass(frozen=True)
class PathResults:
    path: Any
    stats: Any
    rank_biserial_correlation: Any

    def __repr__(self):
        return f"PathResults({' > '.join(self.path)}: {self.stats})"

def analyze_runs(conn, baseline_start, baseline_end, mutant_start, mutant_end, span_path, correlation_threshold=0.1):
    path_string = ' > '.join(span_path)
    print(f"Analyzing path {path_string}")

    baseline_df = fetch_times(conn, baseline_start, baseline_end, span_path)
    mutant_df = fetch_times(conn, mutant_start, mutant_end, span_path)

    print(f"Fetched samples for path {path_string}, baseline: {len(baseline_df)} samples, mutant: {len(mutant_df)} samples")

    test_results = scipy.stats.mannwhitneyu(baseline_df, mutant_df)
    common_language_effect_size = (test_results.statistic / (len(baseline_df) * len(mutant_df))).item()
    rank_biserial_correlation = 2 * common_language_effect_size - 1

    print(f"Tested path {path_string} with result {rank_biserial_correlation=}")
    if abs(rank_biserial_correlation) > correlation_threshold:
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
            return [PathResults(span_path, test_results, rank_biserial_correlation)]
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
