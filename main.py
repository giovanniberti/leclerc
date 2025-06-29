import asyncio
from elasticsearch import AsyncElasticsearch
from aiostream import stream, pipe
import kuzu


ES_ADDRESS = "http://localhost:9201"
MIN_SEARCH_SLICES = 5
client = AsyncElasticsearch(ES_ADDRESS)

async def main():
    try:
        await client.info(
            error_trace=True,
            human=True,
            pretty=True
        )
    except Exception as e:
        print(f"Call to Elastic Info API failed. Are you sure Elasticsearch is running on {ES_ADDRESS}?")
        print(e)
        exit(1)

    pit_keep_alive = "1m"
    point_in_time = (await client.open_point_in_time(
        keep_alive=pit_keep_alive,
        allow_partial_search_results=False,
        index=".ds-traces*"
    ))["id"]

    query = {
        "bool": {
            "must": [
                {
                    "range": {
                        "@timestamp": {
                            "gt": "2025-05-02T12:08:00.000+02:00",
                            "lt": "2025-05-02T16:30:00.000+02:00"
                        }
                    }
                },
                {
                    "query_string": {
                        "query": 'service.name: "API"'
                    }
                }
            ]
        }
    }
    db = kuzu.Database("kuzu-db")
    conn = kuzu.Connection(db)

    conn.execute("CREATE NODE TABLE Span(id STRING, name STRING, trace_id STRING, timestamp TIMESTAMP, duration_us INT64, PRIMARY KEY (id))")
    conn.execute("CREATE REL TABLE HasChild(FROM Span TO Span)")

    search_stream = (
        await search(query, point_in_time)
        | pipe.action(lambda hits: process_hits(hits, db))
    )

    await search_stream
    await client.close_point_in_time(id=point_in_time)


def process_hits(hits, db):
    conn = kuzu.Connection(db)

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


async def search(query, point_in_time):
    nodes = len((await client.nodes.info())["nodes"].keys())
    slices = nodes

    if slices == 1:
        return stream.merge(search_in_slice(query, point_in_time, 0, 1))
    else:
        return stream.merge(*[search_in_slice(query, point_in_time, i, slices) for i in range(slices)])


async def search_in_slice(query, point_in_time, slice, max_slices):
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
