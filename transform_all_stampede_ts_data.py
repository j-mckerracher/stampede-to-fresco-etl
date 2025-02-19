from parallel_etl import ParallelETL


def main():
    etl = ParallelETL(
        base_dir="/data/stampede",
        max_downloaders=8,
        max_processors=12  # Leave 2 cores for system
    )
    etl.start()


if __name__ == "__main__":
    main()
