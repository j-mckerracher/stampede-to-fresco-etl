def run(self):
    """Run the ETL pipeline with immediate per-node processing and uploads"""
    try:
        # Start activity monitor
        self.activity_monitor.start()
        self.activity_monitor.update_activity(activity_type="initialization")

        # 1. Get list of nodes
        nodes = self.get_node_list()
        if not nodes:
            logger.error("No nodes found. Exiting.")
            return

        self.activity_monitor.update_activity(activity_type="found_nodes")

        # 2. Process nodes with parallel processing
        total_nodes = len(nodes)
        logger.info(f"Starting processing of {total_nodes} nodes")

        # Filter nodes that need processing
        nodes_to_process = [node for node in nodes
                            if not self.state_manager.is_node_processed_and_uploaded(node)]
        logger.info(f"Found {len(nodes_to_process)} nodes that need processing")

        if not nodes_to_process:
            logger.info("All nodes are already processed. Nothing to do.")
            return

        # First, validate the data source URL is accessible
        try:
            logger.info(f"Validating data source accessibility: {self.base_url}")
            test_response = self.session.get(self.base_url, timeout=(5, 10))
            test_response.raise_for_status()
            logger.info(f"Data source validation succeeded: status {test_response.status_code}")
        except Exception as e:
            logger.error(f"Data source validation failed: {e}")
            logger.error("Cannot proceed with processing as data source is not accessible")
            return

        # First try with a single node to validate the pipeline
        try:
            first_node = nodes_to_process[0]
            logger.info(f"Testing pipeline with a single node: {first_node}")

            # Use lower level timeouts for the test
            self.downloader.download_timeout = 60
            self.downloader.connect_timeout = 5

            # Try to process one node with detailed logging
            success = self.process_test_node(first_node)

            if not success:
                logger.error(f"Test node {first_node} failed processing - pipeline may have issues")
                logger.error("Continuing with batch processing but expect failures")
            else:
                logger.info(f"Test node {first_node} processed successfully - pipeline is working")

            # Reset timeouts to normal
            self.downloader.download_timeout = 300
            self.downloader.connect_timeout = 10

        except Exception as e:
            logger.error(f"Error testing pipeline with node {first_node}: {e}")
            logger.error("Continuing with batch processing but expect failures")

        # Use a smaller batch size to prevent overwhelming the server
        batch_size = min(self.max_workers, 5)  # Reduced batch size
        total_nodes_to_process = len(nodes_to_process)
        processed_count = 0

        # Process in batches
        for i in range(0, total_nodes_to_process, batch_size):
            batch = nodes_to_process[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1} with {len(batch)} nodes")

            # Process current batch in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(batch_size, self.max_workers)) as executor:
                future_to_node = {executor.submit(self.process_and_upload_node, node): node
                                  for node in batch}

                logger.info(f"Submitted {len(future_to_node)} nodes for processing in current batch")
                self.activity_monitor.update_activity(activity_type="submitted_batch")

                # Track active futures
                active_futures = list(future_to_node.keys())
                completed_futures = []
                start_time = time.time()

                # Process futures with timeout detection
                while active_futures:
                    # Wait for some futures to complete (with timeout)
                    done, active_futures = concurrent.futures.wait(
                        active_futures,
                        timeout=60,  # Check every minute
                        return_when=concurrent.futures.FIRST_COMPLETED
                    )

                    # Process completed futures
                    for future in done:
                        node = future_to_node[future]
                        try:
                            success = future.result(timeout=10)  # Short timeout to get result
                            processed_count += 1

                            # Update progress
                            print_progress(processed_count, total_nodes_to_process,
                                           prefix='Overall Progress:',
                                           suffix=f'({processed_count}/{total_nodes_to_process})')

                            if success:
                                logger.info(
                                    f"Successfully processed node {node} ({processed_count}/{total_nodes_to_process})")
                            else:
                                logger.warning(f"Failed to process node {node}")

                            # Update activity monitor
                            self.activity_monitor.remove_node(node)
                            completed_futures.append(future)

                        except Exception as e:
                            logger.error(f"Error getting result for node {node}: {e}")
                            self.activity_monitor.remove_node(node)
                            completed_futures.append(future)

                    # Check for stalled futures
                    elapsed_time = time.time() - start_time
                    if elapsed_time > self.stall_detection_timeout and active_futures:
                        stalled_nodes = [future_to_node[f] for f in active_futures]
                        logger.warning(f"Detected potential stalled tasks for nodes: {stalled_nodes}")

                        # Check with activity monitor
                        stalled_nodes_info = self.activity_monitor.get_stalled_nodes(self.stall_detection_timeout)
                        if stalled_nodes_info:
                            logger.warning(f"Activity monitor confirms stalled nodes: {stalled_nodes_info}")

                            # Cancel stalled tasks (by adding to completed and removing from active)
                            for future in list(active_futures):
                                node = future_to_node[future]
                                if any(node == n for n, _, _ in stalled_nodes_info):
                                    logger.warning(f"Cancelling stalled task for node {node}")
                                    future.cancel()
                                    self.activity_monitor.remove_node(node)
                                    completed_futures.append(future)
                                    active_futures.remove(future)

                    # If everything is stalled for too long, abort the batch
                    if (elapsed_time > self.stall_detection_timeout * 2 and
                            len(active_futures) == len(future_to_node) and
                            len(completed_futures) == 0):
                        logger.error(f"Entire batch appears stalled after {elapsed_time:.0f}s. Aborting batch.")
                        for future in list(active_futures):
                            future.cancel()
                        break

                # Release session connections after each batch
                self.session.close()
                self.session = requests.Session()
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=self.max_workers * 2,
                    pool_maxsize=self.max_workers * 4,
                    max_retries=self.max_retries,
                    pool_block=True
                )
                self.session.mount('http://', adapter)
                self.session.mount('https://', adapter)

                # Force garbage collection between batches
                gc.collect()

            logger.info(
                f"Completed batch {i // batch_size + 1} - {processed_count}/{total_nodes_to_process} nodes processed")

            # If the batch was completely unsuccessful, adjust parameters for next batch
            if processed_count == 0 and i > 0:
                logger.warning("No successful nodes processed. Adjusting batch size and timeouts.")
                batch_size = max(1, batch_size // 2)
                self.downloader.download_timeout = max(30, self.downloader.download_timeout // 2)
                self.downloader.connect_timeout = max(2, self.downloader.connect_timeout // 2)
                logger.info(f"New batch size: {batch_size}, download timeout: {self.downloader.download_timeout}s")

        logger.info(f"Processing completed! Processed {processed_count} nodes.")
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down gracefully...")
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {e}", exc_info=True)
    finally:
        self.activity_monitor.stop()
        self.session.close()