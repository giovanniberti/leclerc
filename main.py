import asyncio
from elasticsearch import AsyncElasticsearch

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
                            "gt": "2025-04-30T16:12:00.000+02:00",
                            "lt": "2025-04-30T16:30:00.000+02:00"
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

    results = await search(query, point_in_time)
    total_hits = results

    print(f"{total_hits=}")
    await client.close_point_in_time(id=point_in_time)


class GatheringTaskGroup(asyncio.TaskGroup):
    def __init__(self):
        super().__init__()
        self.__tasks = []

    def create_task(self, coro, *, name=None, context=None):
        task = super().create_task(coro, name=name, context=context)
        self.__tasks.append(task)
        return task

    def results(self) -> list:
        return [task.result() for task in self.__tasks]


async def search(query, point_in_time):
    nodes = len((await client.nodes.info())["nodes"].keys())
    slices = nodes
    hits = 0

    if slices == 1:
        hits = await search_in_slice(query, point_in_time, 0, 1)
    else:
        async with GatheringTaskGroup() as tg:
            for i in range(slices):
                tg.create_task(search_in_slice(query, point_in_time, i, slices))

        results = tg.results()
        for r in results:
            hits += r

    return hits


async def search_in_slice(query, point_in_time, slice, max_slices, cb=lambda _: None):
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
        cb(hits)

        point_in_time = resp["pit_id"]
        search_after = resp['hits']['hits'][-1]['sort']

        if hits_size < max_returned_hits:
            break

    print(f"[slice {slice}] {slice_hits} hits")
    return slice_hits



if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
