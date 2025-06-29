import asyncio
from elasticsearch import AsyncElasticsearch
from aiostream import stream, pipe


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

    search_stream = await search(query, point_in_time)
    total_hits = 0

    async def f(hits):
        await process_hits(hits, conn)

    search_stream = (
        await search(query, point_in_time)
        | pipe.action(f)
    )

    await search_stream
    await client.close_point_in_time(id=point_in_time)


async def process_hits(hits, conn):
    print(f"{len(hits)}")


async def search(query, point_in_time):
    nodes = len((await client.nodes.info())["nodes"].keys())
    slices = nodes

    if slices == 1:
        return stream.merge(search_in_slice(query, point_in_time, 0, 1))
    else:
        return stream.merge(*[search_in_slice(query, point_in_time, i, slices) for i in range(slices)])


async def search_in_slice(query, point_in_time, slice, max_slices):
    slice_hits = 0
    pit_keep_alive = "1m"
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
            timeout="60s",
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
        print(f"[slice {slice}, search {i}] {hits_size} hits")

        yield hits

        point_in_time = resp["pit_id"]
        search_after = resp['hits']['hits'][-1]['sort']

        if hits_size < max_returned_hits:
            break

    print(f"[slice {slice}] {slice_hits} hits")



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
