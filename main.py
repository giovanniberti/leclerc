import argparse
import asyncio
from datetime import datetime, timedelta
from elasticsearch import AsyncElasticsearch
from aiostream import stream, pipe
import kuzu
import pytimeparse

from analysis import analyze_runs


async def main():
    parser = argparse.ArgumentParser(
        prog='leclerc',
        description='An analyzer of performance differences between a baseline and a mutant using Elastic APM traces'
    )
    parser.add_argument('-e', '--elastic', help='URL of the Elasticsearch instance containing traces for both the baseline and the mutant')
    parser.add_argument('-b', '--baseline-elastic', help='URL of the Elasticsearch instance containing traces for the baseline')
    parser.add_argument('-m', '--mutant-elastic', help='URL of the Elasticsearch instance containing traces for the mutant')
    
    parser.add_argument('-d', '--database', help='Path to the Kuzu database for already ingested data')
    
    parser.add_argument('-n', '--service-name', help='Name of the service to filter spans for. Corresponding to `service.name` inside OpenTelemetry spans')
    parser.add_argument('-s', '--span-name', help='Name of the span to be tested for performance differences', required=True)

    parser.add_argument('--duration', help='ISO 8601 date time of the start of trace recording. Syntax: NUMBER UNIT where unit is `s`, `m`, `h`. Example: --duration 15m', type=parse_duration)
    
    parser.add_argument('--baseline-start', help='ISO 8601 date time of the start of baseline trace recording', required=True, type=datetime.fromisoformat)

    baseline_end_datetime = parser.add_mutually_exclusive_group()
    baseline_end_datetime.add_argument('--baseline-end', help='ISO 8601 date time of the end of baseline trace recording', type=datetime.fromisoformat)
    baseline_end_datetime.add_argument('--baseline-duration', help='Duration of the baseline trace recording. Syntax: NUMBER UNIT where unit is `s`, `m`, `h`. Example: --baseline-duration 15m. Overrides --duration', type=parse_duration)
    
    parser.add_argument('--mutant-start', help='ISO 8601 date time of the start of mutant trace recording', required=True, type=datetime.fromisoformat)

    mutant_end_datetime = parser.add_mutually_exclusive_group()
    mutant_end_datetime.add_argument('--mutant-end', help='ISO 8601 date time of the end of mutant trace recording', type=datetime.fromisoformat)
    mutant_end_datetime.add_argument('--mutant-duration', help='Duration of the mutant trace recording. Syntax: NUMBER UNIT where unit is `s`, `m`, `h`. Example: --mutant-duration 15m. Overrides --duration', type=parse_duration)
    
    args = parser.parse_args()
    
    if not args.database and not args.elastic and (not args.baseline_elastic or not args.mutant_elastic):
        print("Must specify either --elastic or both --baseline-elastic and --mutant-elastic or --database")
        exit(1)

    if args.duration and args.baseline_duration and args.mutant_duration:
        print("Cannot specify together --duration, --baseline-duration and --mutant-duration")
        exit(1)

    if args.database and (args.elastic or args.baseline_elastic or args.mutant_elastic):
        print("Cannot specify --database with --baseline-elastic or --mutant-elastic")
        exit(1)

    baseline_duration = args.baseline_duration or args.duration
    baseline_end = args.baseline_end or (args.baseline_start + baseline_duration)

    mutant_duration = args.mutant_duration or args.duration
    mutant_end = args.mutant_end or (args.mutant_start + mutant_duration)

    database_path = args.database if args.database else "kuzu-db"
    db = kuzu.Database(database_path)
    conn = kuzu.Connection(db)

    parameters = []
    if args.elastic is not None:
        parameters.append({
            'elastic_address': args.elastic,
            'span_name': args.span_name,
            'start_time': args.baseline_start,
            'end_time': baseline_end,
            'service_name': args.service_name,
            'db': db
        })
        parameters.append({
            'elastic_address': args.elastic,
            'span_name': args.span_name,
            'start_time': args.mutant_start,
            'end_time': mutant_end,
            'service_name': args.service_name,
            'db': db
        })
    else:
        parameters.append({
            'elastic_address': args.baseline_elastic,
            'span_name': args.span_name,
            'start_time': args.baseline_start,
            'end_time': baseline_end,
            'service_name': args.service_name,
            'db': db
        })
        parameters.append({
            'elastic_address': args.mutant_elastic,
            'span_name': args.span_name,
            'start_time': args.mutant_start,
            'end_time': mutant_end,
            'service_name': args.service_name,
            'db': db
        })

    if not args.database:
        conn.execute("CREATE NODE TABLE Span(id STRING, name STRING, trace_id STRING, timestamp TIMESTAMP, duration_us INT64, PRIMARY KEY (id))")
        conn.execute("CREATE REL TABLE HasChild(FROM Span TO Span)")

        for p in parameters:
            await import_traces(**p)

    differing_paths = analyze_runs(
        conn,
        args.baseline_start,
        baseline_end,
        args.mutant_start,
        mutant_end,
        [args.span_name]
    )

    print("\n=== REPORT ===")
    print(f"Differing paths: {len(differing_paths)}\n")
    for d in differing_paths:
        path = ' > '.join(d.path)
        print(f"{path}: rank_biserial_corr = {d.rank_biserial_correlation}")


def parse_duration(s: str) -> timedelta:
    return timedelta(seconds=pytimeparse.parse(s))

async def import_traces(elastic_address, span_name, start_time, end_time, service_name, db):
    client = AsyncElasticsearch(elastic_address)

    try:
        await client.info(
            error_trace=True,
            human=True,
            pretty=True
        )
    except Exception as e:
        print(f"Call to Elastic Info API failed. Are you sure Elasticsearch is running on {elastic_address}?")
        print(e)
        exit(1)

    pit_keep_alive = "1m"
    point_in_time = (await client.open_point_in_time(
        keep_alive=pit_keep_alive,
        allow_partial_search_results=False,
        index=".ds-traces*"
    ))["id"]

    service_name_filter = f"service.name = '{service_name}'" if service_name else '*'
    query = {
        "bool": {
            "must": [
                {
                    "range": {
                        "@timestamp": {
                            "gt": start_time,
                            "lt": end_time
                        }
                    }
                },
                {
                    "query_string": {
                        "query": service_name_filter
                    }
                }
            ]
        }
    }

    search_stream = (
        await search(client, query, point_in_time)
        | pipe.action(lambda hits: process_hits(hits, db))
    )

    await search_stream
    await client.close_point_in_time(id=point_in_time)


def process_hits(hits, db):
    conn = kuzu.Connection(db)
    print("Processing hits...")

    for hit in hits:
        document = hit["_source"]
        parent_id = None

        if "transaction" in document:
            root = document["transaction"]
        else:
            root = document["span"]
            parent_id = document["parent"]["id"]

        operation_name = root["name"]
        duration_us = root["duration"]["us"]
        timestamp = document["@timestamp"]
        trace_id = document["trace"]["id"]
        span_id = document["span"]["id"]

        assert operation_name is not None
        assert duration_us is not None
        assert timestamp is not None
        assert trace_id is not None
        assert span_id is not None

        conn.execute("""
            MERGE (s: Span { id: $span_id })
            ON CREATE SET s.trace_id = $trace_id, s.timestamp = timestamp($timestamp), s.duration_us = $duration_us, s.name = $operation_name
            ON MATCH  SET s.trace_id = $trace_id, s.timestamp = timestamp($timestamp), s.duration_us = $duration_us, s.name = $operation_name
        """, parameters={
            'span_id': span_id,
            'trace_id': trace_id,
            'timestamp': timestamp,
            'duration_us': duration_us,
            'operation_name': operation_name
        })

        if parent_id is not None:
            conn.execute("MERGE (s: Span { id: $parent_id })", parameters={
                'parent_id': parent_id
            })

            conn.execute("""
                MATCH (s1: Span), (s2: Span)
                WHERE s1.id = $parent_id AND s2.id = $span_id
                CREATE (s1)-[:HasChild]->(s2)
            """, parameters={
                'parent_id': parent_id,
                'span_id': span_id
            })

    print("Processing done")


async def search(client, query, point_in_time):
    nodes = len((await client.nodes.info())["nodes"].keys())
    slices = nodes

    if slices == 1:
        return stream.merge(search_in_slice(client, query, point_in_time, 0, 1))
    else:
        return stream.merge(*[search_in_slice(client, query, point_in_time, i, slices) for i in range(slices)])


async def search_in_slice(client, query, point_in_time, slice, max_slices):
    slice_hits = 0
    pit_keep_alive = "5m"
    max_returned_hits = 10_000
    search_after = None
    i = 0
    query_slice = {
        "id": slice,
        "max": max_slices
    }
    slicing_enabled = max_slices > 1

    if not slicing_enabled:
        query_slice = None
    
    while True:
        resp = await client.search(
            pit={"id": point_in_time, "keep_alive": pit_keep_alive},
            allow_partial_search_results=False,
            query=query,
            timeout="5m",
            search_after=search_after,
            size=max_returned_hits,
            sort={
                "@timestamp": {
                    "order": "asc",
                    "format": "strict_date_optional_time_nanos",
                    "numeric_type": "date_nanos"
                }
            },
            slice=query_slice
        )

        hits = resp["hits"]["hits"]
        hits_size = len(hits)
        slice_hits += hits_size

        if hits_size == 0:
            break

        i += 1
        print(f"[slice {slice}, search {i}] {format(hits_size, '_d').replace('_', ' ')} hits")

        yield hits

        point_in_time = resp["pit_id"]
        search_after = resp['hits']['hits'][-1]['sort']

        if hits_size < max_returned_hits:
            break

    print(f"[slice {slice}] Processed {format(slice_hits, '_d').replace('_', ' ')} hits")



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
